import os
from multiprocessing.pool import Pool
from time import sleep, time
from elasticsearch import helpers
from util import data_io
from util.data_io import read_jsonl
from es_util import build_es_action, build_es_client


def pop_exception(d):
    d['index'].pop('exception')
    return d

data = None
def init_fun(data_supplier):
    global data
    data = data_supplier()

def worker_fun(file:str):
    global data
    dicts_g = (d for d in read_jsonl(file, limit=data['limit']))
    actions_g = (build_es_action(d,data['es_index_name'],data['es_type']) for d in dicts_g)

    failed_docs = (pop_exception(d) for ok,d in helpers.streaming_bulk(data['es_client'],actions_g , chunk_size=1_000,
                           yield_ok=False, raise_on_error=True))

    data_io.write_jsonl('failed_%s.jsonl'%file.split('/')[-1].replace('.txt.gz',''), failed_docs)

def populate_es_parallel_pool(files, es_index_name, es_type, limit=None, num_processes = 4, **kwargs):
    def data_supplier():
        data = {
            'es_client':build_es_client(),
            'limit':limit,
            'es_index_name':es_index_name,
            'es_type':es_type
        }
        data.update(kwargs)
        return data

    with Pool(processes=num_processes, initializer=init_fun, initargs=(data_supplier,)) as pool:
        list(pool.imap_unordered(worker_fun, files,chunksize=1))

if __name__ == "__main__":
    INDEX_NAME = "test"
    TYPE = "paper"
    es = build_es_client()

    es.indices.delete(index=INDEX_NAME, ignore=[400, 404])
    es.indices.create(index=INDEX_NAME, ignore=400)
    sleep(3)

    path = '/docker-share/data/MAG_papers'

    start = time()
    files = [path + '/' + file_name for file_name in os.listdir(path) if file_name.endswith('txt.gz')]
    populate_es_parallel_pool(files[:4], INDEX_NAME, TYPE, limit=500_000)
    dur = time()-start

    sleep(3)
    count = es.count(index=INDEX_NAME, doc_type=TYPE, body={"query": {"match_all": {}}})['count']
    print("populating es-index of %d documents took: %0.2f seconds"%(count,dur))
