import os
from elasticsearch import helpers
from tqdm import tqdm
from util import data_io
from util.data_io import read_jsonl
from es_util import build_es_action, build_es_client


def pop_exception(d):
    d['index'].pop('exception')
    return d

if __name__ == "__main__":
    INDEX_NAME = "test"
    TYPE = "paper"
    es = build_es_client()

    es.indices.delete(index=INDEX_NAME, ignore=[400, 404])
    es.indices.create(index=INDEX_NAME, ignore=400)

    path = '/docker-share/data/MAG_papers'
    file_names = [file_name for file_name in os.listdir(path) if file_name.endswith('txt.gz')]
    dicts_g = (d for file_name in file_names for d in read_jsonl(path + '/' + file_name))

    es_actions_g = (build_es_action(d, index_name=INDEX_NAME, es_type=TYPE) for d in dicts_g)
    failed_g = (pop_exception(d) for ok,d in tqdm(helpers.streaming_bulk(es, es_actions_g, chunk_size=1_000, yield_ok=True, raise_on_error=True)) if not ok)
    data_io.write_jsonl('failed.jsonl',failed_g)

    count = es.count(index=INDEX_NAME, doc_type=TYPE, body={"query": {"match_all": {}}})['count']
    print("you've got an es-index of %d documents"%count)
