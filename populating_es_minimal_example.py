import os
from time import sleep

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from util.data_io import read_jsonl


def es_actions_generator(dicts):
    for idx, l in enumerate(dicts):
        doc = {
            '_id': idx,
            '_op_type': 'index',
            # '_op_type': 'create',
            '_index': INDEX_NAME,
            '_type': TYPE,
            '_source': {k:None if isinstance(v,str) and len(v)==0 else v for k,v in l.items()}
        }
        yield (doc)

if __name__ == "__main__":

    INDEX_NAME = "sample-index"
    TYPE = "document"
    # host = 'localhost' # or somewhere else!
    host = 'gunther' # or somewhere else!
    es = Elasticsearch(hosts=[{"host": host, "port": 9200}])
    es.indices.delete(index=INDEX_NAME, ignore=[400, 404])
    es.indices.create(index=INDEX_NAME, ignore=400)

    path = '.'
    file_names = [file_name for file_name in os.listdir(path) if file_name.endswith('.jsonl')]
    dicts_g = (d for file_name in file_names for d in read_jsonl(path + '/' + file_name))

    actions = es_actions_generator(dicts_g)
    bulk(es, actions)

    sleep(3)
    count = es.count(index=INDEX_NAME, doc_type=TYPE, body={"query": {"match_all": {}}})['count']
    print("you've got an es-index of %d documents" % count)
