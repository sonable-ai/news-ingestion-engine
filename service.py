from concurrent import futures
import time
import newspaper
import json
import schedule
import uuid
from opensearchpy import OpenSearch
import os
import hashlib
import sys

sys.path.append("./generated")

import grpc
import generated.Base_pb2 as BaseMessages
import generated.AggregateMessages_pb2 as AggregateMessages
import generated.AggregateService_pb2 as AggregateService_pb2
import generated.AggregateService_pb2_grpc as AggregateService_pb2_grpc
import threading
import logging
import logging.config
from newspaper.mthreading import fetch_news

import nltk
nltk.download("punkt_tab")

from dotenv import load_dotenv
load_dotenv()

index = "articles"
opensearchHost = "opensearch"
opensearchPort = 9200
opensearchAuth = ('admin', os.getenv("OPENSEARCH_INITIAL_ADMIN_PASSWORD"))
opensearch = OpenSearch(
    hosts = [{'host': opensearchHost, 'port': opensearchPort}],
    http_compress = True, # enables gzip compression for request bodies
    http_auth = opensearchAuth,
)

class AggregateService(AggregateService_pb2_grpc.AggregateServiceServicer):
    def requestAggregate(self, request, context):
        query = " ".join(request.tags)
        cached_results = cache_query_results(query, opensearch)        
        for result in cached_results:
            yield AggregateMessages.ArticleData(
                id=0,
                url="",
                title=result["title"],
                content=result["content"],
                tags=result["tags"],
                processedText=result["content"],
            )

def cache_query_results(query, opensearch, cache_index="articles_cache"):
    """
    Cache the results of the query_articles function in OpenSearch.
    
    Args:
        query (str): The search query string.
        opensearch (OpenSearch): OpenSearch client instance.
        cache_index (str): The index name for caching. Default is 'articles_cache'.
    
    Returns:
        list: List of cached or fetched article results.
    """
    # Create a unique hash for the query
    query_hash = hashlib.sha256(query.encode()).hexdigest()
    
    # Ensure cache index exists
    try:
        if not opensearch.indices.exists(cache_index):
            opensearch.indices.create(index=cache_index, body={
                "settings": {
                    "index": {
                        "number_of_shards": 1,
                        "number_of_replicas": 0
                    }
                },
                "mappings": {
                    "properties": {
                        "query_hash": { "type": "keyword" },
                        "results": { "type": "nested" }
                    }
                }
            })
    except Exception as e:
        logging.error(f"Error creating cache index: {e}")
        return []

    # Check for cached results
    try:
        search_response = opensearch.search(index=cache_index, body={
            "query": {
                "term": {"query_hash": query_hash}
            }
        })
        if search_response["hits"]["hits"]:
            logging.info("Cache hit - returning cached results.")
            return search_response["hits"]["hits"][0]["_source"]["results"]
    except Exception as e:
        logging.error(f"Error fetching from cache: {e}")

    # If not cached, query the articles index
    logging.info("Cache miss - querying articles index.")
    search_arr = []
    search_arr.append({"index": "articles"})
    search_arr.append({
        "query": {
            "multi_match": {
                "query": query,
                "fields": ["text", "title", "tags", "keywords"]
            }
        }
    })

    try:
        res = opensearch.msearch(body=search_arr)
        results = []
        for result in res['responses']:
            for hit in result['hits']['hits']:
                results.append({
                    "title": hit['_source']['title'],
                    "content": hit['_source']['text'],
                    "tags": hit['_source']['tags'],
                    "keywords": hit['_source']['keywords']
                })

        # Store results in cache
        try:
            opensearch.index(index=cache_index, id=query_hash, body={
                "query_hash": query_hash,
                "results": results
            })
            logging.info("Results cached successfully.")
        except Exception as e:
            logging.error(f"Error caching results: {e}")

        return results
    except Exception as e:
        logging.error(f"Error querying articles index: {e}")
        return []


def store_article_data(articles):
    ret = opensearch.bulk(body=articles)
    if ret["errors"]:
        logging.error("There were errors.")
        for error in ret["errors"]:
            logging.error(f"{error['index']['status']}: {error['index']['error']['type']}")
    else:
        logging.error(f"Bulk inserted {len(ret['items'])} items.")


def query_articles():
    logging.info("Fetching articles...")

    try:
        ret = opensearch.indices.get("articles")
        if not ret:
            raise Exception("no response")
    except:
        opensearch.indices.create(index, body={
            "settings": {
                "index": {
                    "number_of_shards": 2,
                    "number_of_replicas": 1
                }
            },
            "mappings": {
                "properties": {
                    "title": { "type": "text" },
                    "text": { "type": "text" },
                    "tags": { "type": "keyword" },
                    "keywords": { "type": "keyword"},
                    "date": { "type": "date" }
                }
            },
            "aliases": {
                "articles-alias": {}
            }
        })

    with open('sources.json', 'r') as file:
        data = json.load(file)
        papers = []
        logging.info("Building sources...")
        logging.info("Fetching news -- this may take a while...")
        for source in data["sources"]:
            logging.info(source["name"])
            papers = []
            papers.append(newspaper.build(source["url"], max_keywords=30))
            fetch_news(papers, threads=4, )
            for paper in papers:
                logging.info(f"Parsing {len(paper.articles)} articles...")
                counter = 0
                article_data = []
                for article in paper.articles:
                    if counter % 10 == 0 and counter != 0:
                        logging.info(f"{counter}/{len(paper.articles)}")
                        store_article_data(article_data)
                        article_data = []
                    try:
                        counter += 1
                        article.nlp()
                    except Exception as e:
                        logging.error(e)
                    logging.info("Parsed " + article.title)

                    article_data.append({
                        "index": {
                            "_index": index, 
                            "_id": hash(article.url) # url as id
                        }
                    })
                    article_data.append({
                        "text": article.text, 
                        "title": article.title, 
                        "tags": article.tags if article.tags else [], 
                        "keywords": article.keywords if article.keywords else [],
                        "date": article.publish_date 
                    })
            logging.error(f"Done. Inserting {len(article_data)} into OpenSearch...")
            if len(article_data) != 0:
                store_article_data(article_data)

def start_crawler():
    #query_articles()
    schedule.every().hour.do(query_articles)
    logging.info("Scheduled crawler daemon")
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__=="__main__":
    logging.basicConfig(
        level=logging.DEBUG,  # Log everything for testing
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
        logging.FileHandler("info.log", mode="a"),  # Append mode
        logging.StreamHandler(),  # Console logging
        ],
    )

    logging.config.fileConfig("logging.conf")
    logging.info("Waiting 60 seconds to start...")
    time.sleep(30)
    t = threading.Thread(target=start_crawler, daemon=True)
    t.start()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    AggregateService_pb2_grpc.add_AggregateServiceServicer_to_server(AggregateService(), server)
    server.add_insecure_port('[::]:50052')
    server.start()
    logging.info("Started GRPC listener")
    server.wait_for_termination()
    while True:
        if not t.is_alive():
            t.run()