from time import sleep, time
import os
from aminer_to_es_parallel_pool import populate_es_parallel_pool
from es_util import build_es_client

if __name__ == "__main__":
    INDEX_NAME = "semanticscholar"
    TYPE = "paper"
    es = build_es_client()

    # es.indices.delete(index=INDEX_NAME, ignore=[400, 404])
    es.indices.create(index=INDEX_NAME, ignore=400)
    sleep(3)

    path = '/docker-share/data/semantic_scholar'

    files = [path + '/' + file_name for file_name in os.listdir(path) if file_name.startswith('s2') and file_name.endswith('.gz')]
    num_processes = 8
    populate_es_parallel_pool(files, INDEX_NAME, TYPE,num_processes=num_processes)