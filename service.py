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

index = "articles-nlp"
opensearchHost = "opensearch"
opensearchPort = 9200
opensearchAuth = ('admin', os.getenv("OPENSEARCH_INITIAL_ADMIN_PASSWORD"))

opensearch = None

model_id = ""

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

def deploy_model(model_id):
    logging.info("Deploying ML model...")
    try:
        model_deploy_result = opensearch.plugins.ml.deploy_model(model_id)
        model_deploy_status = model_deploy_result["status"]
        model_deploy_task_id = model_deploy_result["task_id"]
        logging.error(json.dumps(model_deploy_result))
        while model_deploy_status != "COMPLETED" and model_deploy_status != "FAILED":
            res = opensearch.plugins.ml.get_task(model_deploy_task_id)
            logging.error(json.dumps(res))
            model_deploy_status = res["state"]
    except Exception as e:
        logging.info(e)

def init_opensearch_model():
    global model_id
    model_id = ""
    
    logging.info("Preparing ML settings for OpenSearch...")
    opensearch.cluster.put_settings(
        body={
            "persistent": {
                "plugins.ml_commons.only_run_on_ml_node": "false",
                "plugins.ml_commons.model_access_control_enabled": "true",
                "plugins.ml_commons.native_memory_threshold": "99"
            }
        }
    )
    logging.info("Registering model group...")
    model_group_result = opensearch.plugins.ml.register_model_group(
        body={
            "name": str(uuid.uuid4()),
            "description": "A model group for NLP models",
        }
    )
    model_group_id = model_group_result["model_group_id"]
    logging.info("Registering ML model...")
    model_register_result = opensearch.plugins.ml.register_model(
        body={
            "name": "huggingface/sentence-transformers/msmarco-distilbert-base-tas-b",
            "version": "1.0.1",
            "model_group_id": model_group_id,
            "model_format": "TORCH_SCRIPT"
        }
    )

    #Wait for task to complete
    model_register_task_id = model_register_result["task_id"]
    model_register_status = model_register_result["status"]
    while model_register_status != "COMPLETED":
        logging.info("...")
        res = opensearch.plugins.ml.get_task(model_register_task_id)
        logging.info(res)
        model_register_status = res["state"]
        if model_register_status == "COMPLETED":
            model_id=res["model_id"]
            break
        time.sleep(3)

    logging.info("Saving registered model id to model.info...")
    with open("model.info", "x") as f:
        f.write(model_id)
        f.close()
    return model_id

def init_opensearch():
    logging.info("Initializing OpenSearch...")
    global opensearch
    global model_id
    if opensearch is None:
        opensearch = OpenSearch(
            hosts = [{'host': opensearchHost, 'port': opensearchPort}],
            http_compress = True, # enables gzip compression for request bodies
            http_auth = opensearchAuth,
        )
    

    try:
        with open('model.info', 'r') as f:
            model_id = f.read()
            f.close()
    except Exception as e:
        logging.info("No model.info found.")

    if (model_id == ""):
        model_id = init_opensearch_model()
    
    deploy_model(model_id)

    try: 
        res = opensearch.ingest.get_pipeline(id="articles-pipeline")
        logging.info(res)
    except:
        logging.info("No existing pipeline found")
        logging.info("Creating Ingest pipeline...")
        opensearch.ingest.put_pipeline(
            id="articles-pipeline",
            body={
                "description": "An NLP ingest pipeline",
                "processors": [
                    {
                        "text_embedding": {
                            "model_id": model_id,
                            "field_map": {
                                "content": "content_embedding",
                                "title": "title_embedding",
                            }
                        }
                    }
                ]
            }
        )

    try:
        res = opensearch.indices.get(index)
        logging.info(res)
    except:
        logging.info("Creating articles index...")
        opensearch.indices.create(index, body={
            "settings": {
                "index": {
                    "knn": True,
                    "number_of_shards": 2,
                    "number_of_replicas": 1
                },
                "default_pipeline": "articles-pipeline"
            },
            "mappings": {
                "properties": {
                    "content": { "type": "text" },
                    "title": { "type": "text" },
                    "tags": { "type": "keyword" },
                    "keywords": { "type": "keyword"},
                    "date": { "type": "date" },

                    "content_embedding": {
                        "type": "knn_vector",
                        "dimension": 768,
                        "method": {
                            "engine": "lucene",
                            "space_type": "l2",
                            "name": "hnsw",
                            "parameters": {}
                        }
                    },
                    "title_embedding": {
                        "type": "knn_vector",
                        "dimension": 768,
                        "method": {
                            "engine": "lucene",
                            "space_type": "l2",
                            "name": "hnsw",
                            "parameters": {}
                        }
                    },
                }
            },
            "aliases": {
                "articles-alias": {}
            }
        })
    logging.info("Done!")
    return opensearch

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
    global model_id
    search_arr = []
    search_arr.append({"index": index})
    search_arr.append({
    })

    try:
        res = opensearch.search(index=index, body={
            "query": {
                "neural": {
                    "content_embedding": {
                        "query_text": query,
                        "model_id": model_id,
                        "k": 5
                    }
                }
            }
        },
        params = {
            "timeout": 40
        })
        results = []
        logging.info("RESULT " + json.dumps(res))
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
    ret = opensearch.bulk(body=articles, timeout=30)
    if ret["errors"]:
        logging.error("There were errors.")
        logging.error(ret)
    else:
        logging.error(f"Bulk inserted {len(ret['items'])} items.")


def query_articles():
    logging.info("Fetching articles...")

    ret = opensearch.indices.get(index)
    if not ret:
        raise Exception("no response")

    with open('sources.json', 'r') as file:
        data = json.load(file)
        papers = []
        logging.info("Building sources...")
        logging.info("Fetching news -- this may take a while...")
        for source in data["sources"]:
            logging.info(source["name"])
            papers = []
            papers.append(newspaper.build(source["url"], max_keywords=30, fetch_images=False, language="en"))
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

    logging.info("Initializing OpenSearch connection...")

    opensearchInit = False
    while opensearchInit is not True:
        try:
            init_opensearch()
            opensearchInit = True
        except Exception as e:
            logging.error("Failed to connect to OpenSearch. Retrying in 10 seconds...")
            logging.error(e)
            time.sleep(10)

    logging.info("opensearch init")
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