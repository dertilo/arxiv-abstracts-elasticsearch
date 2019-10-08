import os
from elasticsearch import helpers
from tqdm import tqdm
from util import data_io
from util.data_io import read_jsonl
from es_util import build_es_action, build_es_client


def pop_exception(d):
    d['index'].pop('exception')
    return d

def populate_es_streaming_bulk(es,files,es_index_name,es_type,limit=None,chunk_size = 1000):
    dicts_g = (d for file in files for d in read_jsonl(file,limit=limit))
    es_actions_g = (build_es_action(d, index_name=es_index_name, es_type=es_type) for d in dicts_g)
    results_g = helpers.streaming_bulk(es, es_actions_g, chunk_size=chunk_size, yield_ok=True, raise_on_error=True)
    failed_g = (pop_exception(d) for ok, d in tqdm(results_g) if not ok)
    data_io.write_jsonl('failed.jsonl', failed_g)

if __name__ == "__main__":
    INDEX_NAME = "test"
    TYPE = "paper"
    es = build_es_client()

    es.indices.delete(index=INDEX_NAME, ignore=[400, 404])
    es.indices.create(index=INDEX_NAME, ignore=400)

    path = '/docker-share/data/MAG_papers'
    files = [path + '/' + file_name for file_name in os.listdir(path) if file_name.endswith('txt.gz')]
    populate_es_streaming_bulk(es,files,INDEX_NAME,TYPE,limit=1000_0000)

    count = es.count(index=INDEX_NAME, doc_type=TYPE, body={"query": {"match_all": {}}})['count']
    print("you've got an es-index of %d documents"%count)
