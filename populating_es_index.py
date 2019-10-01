import gzip
import json
import sys

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

def read_jsonslines(file, limit=sys.maxsize):
    with gzip.open(file, mode="rb") if file.endswith('.gz') else open(file, mode="rb") as f:
        counter=0
        for line in f:
            counter += 1
            if counter > limit: break
            yield json.loads(line.decode('utf-8'))

def populate_es_database(file):
    def doc_generator():
        for idx, l in enumerate(read_jsonslines(file)):
            doc = {
                '_id': idx,
                '_op_type': 'index',#'create',
                '_index': INDEX_NAME,
                '_type': TYPE,
                '_source': {k:None if isinstance(v,str) and len(v)==0 else v for k,v in l.items()}
            }
            yield (doc)

    bulk(es, doc_generator())

if __name__ == "__main__":

    INDEX_NAME = "documents"
    TYPE = "document"
    host = 'localhost' # or somewhere else!
    es = Elasticsearch(hosts=[{"host": host, "port": 9200}])
    populate_es_database('sample_data.jsonl')
    count = es.count(index=INDEX_NAME, doc_type=TYPE, body={"query": {"match_all": {}}})['count']
    print("you've got an es-index of %d documents"%count)
