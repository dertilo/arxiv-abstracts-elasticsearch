from typing import Iterable, Dict
from elasticsearch import helpers, Elasticsearch
from tqdm import tqdm
from util import data_io
from esutil.es_util import build_es_action


def populate_es_streaming_bulk(
    es_client: Elasticsearch,
    dicts: Iterable[Dict],
    es_index_name: str,
    es_type: str,
    chunk_size: int = 500,
):
    def pop_exception(d):
        d["index"].pop("exception")
        return d

    es_actions_g = (
        build_es_action(d, index_name=es_index_name, es_type=es_type) for d in dicts
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
