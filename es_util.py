import hashlib
import json
import multiprocessing
import traceback
from queue import Queue
from typing import List

from elasticsearch import Elasticsearch
from elasticsearch.helpers import expand_action, _process_bulk_chunk, _chunk_actions


def build_es_action(datum, index_name, es_type, op_type='index'):
    _source = {k: None if isinstance(v, str) and len(v) == 0 else v for k, v in datum.items()}
    doc = {
        '_id': hashlib.sha1(json.dumps(_source).encode('utf-8')).hexdigest(),
        '_op_type': op_type,
        '_index': index_name,
        '_type': es_type,
        '_source': _source
    }
    return doc


def build_es_client(
        host = 'localhost',port=9200
    ):
    es = Elasticsearch(hosts=[{"host": host, "port": port}],timeout=30)
    es.cluster.health(wait_for_status='yellow', request_timeout=1)
    return es