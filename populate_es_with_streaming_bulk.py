import os
from typing import List

from elasticsearch import helpers, Elasticsearch
from tqdm import tqdm
from util import data_io
from util.data_io import read_jsonl
from es_util import build_es_action, build_es_client


def pop_exception(d):
    d["index"].pop("exception")
    return d


def populate_es_streaming_bulk(
    es_client: Elasticsearch,
    files: List[str],
    es_index_name: str,
    es_type: str,
    limit: int = None,
    chunk_size: int = 100,
):
    dicts_g = (d for file in files for d in read_jsonl(file, limit=limit))
    es_actions_g = (
        build_es_action(d, index_name=es_index_name, es_type=es_type) for d in dicts_g
    )
    results_g = helpers.streaming_bulk(
        es_client,
        es_actions_g,
        chunk_size=chunk_size,
        yield_ok=True,
        raise_on_error=True,
    )
    failed_g = (pop_exception(d) for ok, d in tqdm(results_g) if not ok)
    data_io.write_jsonl("failed.jsonl", failed_g)


if __name__ == "__main__":
    INDEX_NAME = "test"
    TYPE = "paper"
    es_client = build_es_client(host="guntherhamachi")

    es_client.indices.delete(index=INDEX_NAME, ignore=[400, 404])
    es_client.indices.create(index=INDEX_NAME, ignore=400)

    from pathlib import Path
    home = str(Path.home())
    # path = "/docker-share/data/semantic_scholar"
    path = home+"/gunther/data/semantic_scholar"
    files = [
        path + "/" + file_name
        for file_name in os.listdir(path)
        if file_name.startswith("s2") and file_name.endswith(".gz")
    ]

    populate_es_streaming_bulk(es_client, files, INDEX_NAME, TYPE,chunk_size=100, limit=1000)

    count = es_client.count(
        index=INDEX_NAME, doc_type=TYPE, body={"query": {"match_all": {}}}
    )["count"]
    print("you've got an es-index of %d documents" % count)
