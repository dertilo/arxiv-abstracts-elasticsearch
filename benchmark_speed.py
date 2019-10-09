import os
from time import time, sleep
from util import data_io
from aminer_to_es_parallel_bulk import populate_es_parallel_bulk
from aminer_to_es_parallel_pool import populate_es_parallel_pool
from aminer_to_es_streaming_bulk import populate_es_streaming_bulk
from es_util import build_es_client

INDEX_NAME = "test"
TYPE = "paper"

def pop_exception(d):
    d['index'].pop('exception')
    return d

def benchmark_speed(populate_fun):
    es = build_es_client()
    es.indices.delete(index=INDEX_NAME, ignore=[400, 404])
    es.indices.create(index=INDEX_NAME, ignore=400)
    sleep(3)
    count = es.count(index=INDEX_NAME, doc_type=TYPE, body={"query": {"match_all": {}}})['count']
    assert count==0
    sleep(30) # give the es-"cluster" some time to recover
    start = time()
    populate_fun()
    dur = time()-start
    sleep(3)
    count = es.count(index=INDEX_NAME, doc_type=TYPE, body={"query": {"match_all": {}}})['count']
    print("populating es-index of %d documents took: %0.2f seconds"%(count,dur))
    speed = float(count) / dur
    return speed

if __name__ == "__main__":

    path = '/docker-share/data/MAG_papers'
    files = [path + '/' + file_name for file_name in os.listdir(path) if file_name.endswith('txt.gz') and 'mag_papers_10.txt.gz'!=file_name]
    limit = 2000_000
    speeds = []
    speed = benchmark_speed(
        lambda: populate_es_streaming_bulk(build_es_client(), [files[0]], INDEX_NAME, TYPE, limit=limit))
    speeds.append({'method':'streaming','speed': speed,'num-processes':1})
    print('streaming-speed: %0.2f docs per second' % speed)
    for num_processes in [1,2,4,8]:
        speed = benchmark_speed(
            lambda: populate_es_parallel_bulk(build_es_client(), [files[0]], INDEX_NAME, TYPE, limit=limit,
                                              num_processes=num_processes))
        speeds.append({'method':'parallel-bulk','speed': speed,'num-processes':num_processes})
        print('%d processes parallel-bulk-speed: %0.2f docs per second' % (num_processes, speed))
        speed = benchmark_speed(
            lambda: populate_es_parallel_pool(files[:num_processes], INDEX_NAME, TYPE, limit=int(limit / num_processes),
                                              num_processes=num_processes, from_scratch=True))
        speeds.append({'method':'parallel-pool','speed': speed,'num-processes':num_processes})
        print('%d processes parallel-pool-speed: %0.2f docs per second' % (num_processes, speed))

    benchmark_jsonl = 'speed_benchmark.jsonl'
    data_io.write_jsonl(benchmark_jsonl, speeds)

