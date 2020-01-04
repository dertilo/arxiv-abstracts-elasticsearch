import os
from pathlib import Path
from typing import List

import pandas
from time import time, sleep

from util.data_io import read_jsonl

from es_parallel_bulk import populate_es_parallel_bulk
from es_parallel_pool import populate_es_parallel_pool

from esutil.es_streaming_bulk import populate_es_streaming_bulk
from esutil.es_util import build_es_client

INDEX_NAME = "test"
TYPE = "paper"


def pop_exception(d):
    d["index"].pop("exception")
    return d


def benchmark_speed(populate_fun):
    es = build_es_client()
    es.indices.delete(index=INDEX_NAME, ignore=[400, 404])
    es.indices.create(index=INDEX_NAME, ignore=400)
    sleep(3)
    count = es.count(
        index=INDEX_NAME, doc_type=TYPE, body={"query": {"match_all": {}}}
    )["count"]
    assert count == 0
    sleep(30)  # give the es-"cluster" some time to recover
    start = time()
    populate_fun()
    dur = time() - start
    sleep(3)
    count = es.count(
        index=INDEX_NAME, doc_type=TYPE, body={"query": {"match_all": {}}}
    )["count"]
    print("populating es-index of %d documents took: %0.2f seconds" % (count, dur))
    speed = float(count) / dur
    return speed


def get_files():
    home = str(Path.home())
    path = home + "/data/semantic_scholar"
    files = [
        path + "/" + file_name
        for file_name in os.listdir(path)
        if file_name.startswith("s2") and file_name.endswith(".gz")
    ]
    return files


def give_es_some_time():
    # not sure whether this is needed
    sleep(10)


if __name__ == "__main__":

    files = get_files()

    limit = 10_000
    speeds = []

    def benchmark_speed_print_and_append(
        populate_fun, method_name: str, num_processes: int
    ):
        speed = benchmark_speed(populate_fun)
        speeds.append(
            {"method": method_name, "speed": speed, "num-processes": num_processes}
        )
        print(
            "%d processes %s-speed: %0.2f docs per second"
            % (num_processes, method_name, speed)
        )

    speed = benchmark_speed(
        lambda: populate_es_streaming_bulk(
            build_es_client(),
            (d for d in read_jsonl(files[0], limit=limit)),
            INDEX_NAME,
            TYPE,
        )
    )
    speeds.append({"method": "streaming", "speed": speed, "num-processes": 1})
    print("streaming-speed: %0.2f docs per second" % speed)
    give_es_some_time()

    for num_processes in [1, 2, 4]:
        give_es_some_time()
        benchmark_speed_print_and_append(
            populate_fun=lambda: populate_es_parallel_bulk(
                build_es_client(),
                [files[0]],
                INDEX_NAME,
                TYPE,
                limit=limit,
                num_processes=num_processes,
                chunk_size=1000,
            ),
            num_processes=num_processes,
            method_name="parallel-bulk",
        )
        give_es_some_time()
        benchmark_speed_print_and_append(
            populate_fun=lambda: populate_es_parallel_pool(
                files[:num_processes],
                INDEX_NAME,
                TYPE,
                limit=int(limit / num_processes),
                num_processes=num_processes,
                chunk_size=1000,
            ),
            num_processes=num_processes,
            method_name="parallel-pool",
        )

    data = [{d["method"]: d["speed"], "n": d["num-processes"]} for d in speeds]
    df = pandas.DataFrame(data=data)
    ax = df.plot.bar(x="n", width=1)
    ax.set_xlabel("number of processes")
    ax.set_ylabel("indexing speed in docs/sec")
    from matplotlib import pyplot as plt

    plt.savefig("benchmarking_indexing_speed.png")
