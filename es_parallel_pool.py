import multiprocessing
import os
from pathlib import Path
from time import sleep, time
from elasticsearch import helpers
from util import data_io
from util.consume_with_pool import pool_consume

from esutil.es_util import build_es_client, build_es_action


def pop_exception(d):
    if "exception" in d["index"]:
        d["index"].pop("exception")
    return d


def populate_es_parallel_pool(
    files, es_index_name, es_type, limit=None, num_processes=4, chunk_size=500
):
    assert len(files) >= num_processes

    def consumer_supplier():
        es_client = build_es_client()

        def consumer(file):
            print(
                "%s is doing %s; limit: %d"
                % (multiprocessing.current_process(), file, limit)
            )

            dicts_g = (d for d in data_io.read_jsonl(file, limit=limit))

            actions_g = (
                build_es_action(d, es_index_name, es_type, op_type="index")
                for d in dicts_g
            )
            results_g = helpers.streaming_bulk(
                es_client,
                actions_g,
                chunk_size=chunk_size,
                yield_ok=True,
                raise_on_error=False,
                raise_on_exception=False,
            )

            failed_g = (pop_exception(d) for ok, d in results_g if not ok)
            data_io.write_jsonl(
                "%s_failed.jsonl" % multiprocessing.current_process(), failed_g
            )

        return consumer

    if num_processes > 1:
        pool_consume(
            data=files, consumer_supplier=consumer_supplier, num_processes=num_processes
        )
    else:
        consumer = consumer_supplier()
        [consumer(file) for file in files]


def get_files():
    home = str(Path.home())
    path = home + "/data/semantic_scholar"
    files = [
        path + "/" + file_name
        for file_name in os.listdir(path)
        if file_name.startswith("s2") and file_name.endswith(".gz")
    ]
    return files


if __name__ == "__main__":
    INDEX_NAME = "test-parallel-pool"
    TYPE = "paper"

    es_client = build_es_client()

    es_client.indices.delete(index=INDEX_NAME, ignore=[400, 404])
    es_client.indices.create(index=INDEX_NAME, ignore=400)

    files = get_files()

    start = time()
    num_processes = 8
    populate_es_parallel_pool(
        files[:8], INDEX_NAME, TYPE, limit=10_000, num_processes=num_processes
    )
    dur = time() - start

    sleep(5)
    count = es_client.count(index=INDEX_NAME, doc_type=TYPE)["count"]
    print(
        "populating %s now containing %d documents took: %0.2f seconds"
        % (INDEX_NAME, count, dur)
    )
