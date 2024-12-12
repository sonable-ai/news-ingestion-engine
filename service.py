from concurrent import futures
import time
import newspaper
import json
import schedule
import uuid
from opensearchpy import OpenSearch
import os

import sys
sys.path.append("./generated")

import grpc
import generated.Base_pb2 as BaseMessages
import generated.AggregateMessages_pb2 as AggregateMessages
import generated.AggregateService_pb2 as AggregateService_pb2
import generated.AggregateService_pb2_grpc as AggregateService_pb2_grpc
import threading
import logging
from newspaper.mthreading import fetch_news

from dotenv import load_dotenv
load_dotenv()

index = "articles"
opensearchHost = "opensearch"
opensearchPort = 9200
opensearchAuth = ('admin', os.environ["OPENSEARCH_INITIAL_ADMIN_PASSWORD"])
opensearch = OpenSearch(
    hosts = [{'host': opensearchHost, 'port': opensearchPort}],
    http_compress = True, # enables gzip compression for request bodies
    http_auth = opensearchAuth,
)

class AggregateService(AggregateService_pb2_grpc.AggregateServiceServicer):
    def RequestAggregate(self, request, context):
        search_arr = []
        for tag in request['tags']:
            search_arr.append({"index": "articles"})
            search_arr.append({"query": {"match": {"text": tag}}})
        res = opensearch.msearch(body=search_arr)
        logging.error(res)
        arr = []
        for result in res['responses']:
            for hit in result['hits']:
                arr.append(AggregateMessages.ArticleData(
                    id=0,
                    source=AggregateMessages.DataSource(
                        id=0,
                        name="",
                        baseUrl="",
                    ),
                    url="",
                    title=hit['title'],
                    content=hit['text'],
                    tags=[],
                    processedText=hit['text'],
                    date=BaseMessages.Date(
                        year=0,
                        month=0,
                        day=0
                    ),
                    type=0
                ))
        return arr

def store_article_data(articles):
    ret = opensearch.bulk(body=articles)
    if ret["errors"]:
        logging.error("There were errors.")
        for error in ret["errors"]:
            logging.error(f"{error['index']['status']}: {error['index']['error']['type']}")
    else:
        logging.error(f"Bulk inserted {len(ret['items'])} items.")


def query_articles():
    logging.error("Fetching articles...")

    with open('sources.json', 'r') as file:
        data = json.load(file)
        papers: newspaper.Source = []
        logging.error("Building sources...")
        for source in data["sources"]:
            logging.error(source["name"])
            papers.append(newspaper.build(source["url"]))
        logging.error("Fetching news -- this may take a while...")
        fetch_news(papers, threads=4)
        for paper in papers:
            logging.error(f"Parsing {len(paper.articles)} articles...")
            counter = 0
            article_data = []
            for article in paper.articles:
                if counter % 10 == 0 and counter != 0:
                    logging.error(f"{counter}/{len(paper.articles)}")
                    store_article_data(article_data)
                    article_data = []
                try:
                    counter += 1
                    article.download(recursion_counter=2)
                    article.parse()
                    time.sleep(3)
                except Exception as e:
                    logging.error(e)
                logging.error("Downloaded " + article.title)
                article_data.append({"index": {"_index": index, "_id": hash(article.url)}}) # url as id
                article_data.append({"text": article.text, "title": article.title, "tags": list(article.tags) if article.tags else []})
        logging.error(f"Done. Inserting {len(article_data)} into OpenSearch...")
        if len(article_data) != 0:
            store_article_data(article_data)

def start_crawler():
    query_articles()
    schedule.every().hour.do(query_articles)
    logging.error("Scheduled crawler daemon")
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__=="__main__":
    logging.error("Waiting 60 seconds to start...")
    time.sleep(30)
    t = threading.Thread(target=start_crawler, daemon=True)
    t.start()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    AggregateService_pb2_grpc.add_AggregateServiceServicer_to_server(AggregateService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    logging.error("Started GRPC listener")
    server.wait_for_termination()
    while True:
        if not t.is_alive():
            t.run()