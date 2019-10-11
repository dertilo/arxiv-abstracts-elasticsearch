import multiprocessing
import os
from time import sleep, time
from elasticsearch import helpers
from util import data_io
from util.consume_with_pool import pool_consume

from es_util import build_es_action, build_es_client


def pop_exception(d):
    if 'exception' in d['index']:
        d['index'].pop('exception')
    return d

def populate_es_parallel_pool(files, es_index_name, es_type, limit=None, num_processes = 4, from_scratch=False,**kwargs):

    def consumer_supplier():
        es_client = build_es_client()

        def consumer(file):
            print('%s is working on file: %s'%(multiprocessing.current_process(),file))
            num_to_skip = 0
            dicts_g = (d for d in data_io.read_jsonl(file, limit=limit, num_to_skip=num_to_skip))

            actions_g = (build_es_action(d, es_index_name, es_type, op_type='index') for d in dicts_g)
            results_g = helpers.streaming_bulk(es_client, actions_g,
                                               chunk_size=1_000,
                                               yield_ok=True,
                                               raise_on_error=False,
                                               raise_on_exception=False)

            for k, (ok, d) in enumerate(results_g):
                if not ok and 'index' in d:
                    print('shit')

        return consumer

    pool_consume(
        data=files,
        consumer_supplier=consumer_supplier,
        num_processes=num_processes)

if __name__ == "__main__":
    INDEX_NAME = "s2papers"
    TYPE = "paper"
    es = build_es_client()

    es.indices.delete(index=INDEX_NAME, ignore=[400, 404])
    es.indices.create(index=INDEX_NAME, ignore=400)
    sleep(3)

    path = '/docker-share/data/semantic_scholar'
    files = [path + '/' + file_name for file_name in os.listdir(path) if file_name.startswith('s2') and file_name.endswith('.gz')]

    start = time()
    num_processes = 8
    populate_es_parallel_pool(files[:num_processes], INDEX_NAME, TYPE,limit=1000_000,num_processes=num_processes)
    dur = time()-start

    sleep(3)
    count = es.count(index=INDEX_NAME, doc_type=TYPE, body={"query": {"match_all": {}}})['count']
    print("populating %s now containing %d documents took: %0.2f seconds"%(INDEX_NAME,count,dur))
