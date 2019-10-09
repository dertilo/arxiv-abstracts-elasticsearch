import multiprocessing
import os
import pathlib
import shutil
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
    logs_file = get_logs_file_name(file,data['es_index_name'],data['es_type'])
    if data['from_scratch'] and os.path.isfile(logs_file):
        os.remove(logs_file)
    num_to_skip = get_num_to_skip(logs_file)
    dicts_g = (d for d in data_io.read_jsonl(file, limit=data['limit'],num_to_skip=num_to_skip))

    actions_g = (build_es_action(d,data['es_index_name'],data['es_type'],data['op_type']) for d in dicts_g)
    results_g = helpers.streaming_bulk(data['es_client'],actions_g ,
                                       chunk_size=1_000,
                                       yield_ok=True,
                                       raise_on_error=False,
                                       raise_on_exception=False)

    for k,(ok, d) in enumerate(results_g):
        if not ok and 'index' in d:
            fail = pop_exception(d)
            data_io.write_jsonl(logs_file,[fail],mode='ab')
        elif k%100_000==0:
            data_io.write_jsonl(logs_file,[{'num_indexed':num_to_skip+k}],mode='ab')

def get_num_to_skip(logs_file):
    num_to_skip = 0
    if os.path.isfile(logs_file):
        dicts = [d for d in data_io.read_jsonl(logs_file) if 'num_indexed' in d]
        if len(dicts) > 0:
            num_to_skip = dicts[-1]['num_indexed']
            print('skipped %d for %s'%(num_to_skip,logs_file))
    return num_to_skip

def get_logs_file_name(file,es_index_name,es_type):
    log_dir = '%s_%s_logs'%(es_index_name,es_type)
    pathlib.Path(log_dir).mkdir(parents=True, exist_ok=True)
    return log_dir+'/es_indexing_logs_%s.jsonl' % file.split('/')[-1].replace('.txt.gz', '')

def populate_es_parallel_pool(files, es_index_name, es_type, limit=None, num_processes = 4, from_scratch=False,**kwargs):
    def data_supplier():
        data = {
            'es_client':build_es_client(),
            'limit':limit,
            'es_index_name':es_index_name,
            'es_type':es_type,
            'op_type':'index',
            'from_scratch':from_scratch
        }
        data.update(kwargs)
        return data

    with Pool(processes=num_processes, initializer=init_fun, initargs=(data_supplier,)) as pool:
        list(pool.imap_unordered(worker_fun, files,chunksize=1))

if __name__ == "__main__":
    INDEX_NAME = "documents"
    TYPE = "paper"
    es = build_es_client()

    # es.indices.delete(index=INDEX_NAME, ignore=[400, 404])
    es.indices.create(index=INDEX_NAME, ignore=400)
    sleep(3)

    path = '/docker-share/data/MAG_papers'

    start = time()
    files = [path + '/' + file_name for file_name in os.listdir(path) if file_name.endswith('txt.gz')]
    num_processes = 8
    populate_es_parallel_pool(files, INDEX_NAME, TYPE,num_processes=num_processes)
    dur = time()-start

    sleep(3)
    count = es.count(index=INDEX_NAME, doc_type=TYPE, body={"query": {"match_all": {}}})['count']
    print("populating es-index of %d documents took: %0.2f seconds"%(count,dur))

'''
populating es-index of 208915369 documents took: 16312.36 seconds
'''
