import hashlib
import json

from elasticsearch import Elasticsearch


def build_es_action(datum, index_name, es_type, op_type="index"):
    def empty_string_to_None(v):
        return None if isinstance(v, str) and len(v) == 0 else v

    _source = {
        k: empty_string_to_None(v) for k, v in datum.items()
    }
    doc = {
        "_id": hashlib.sha1(json.dumps(_source).encode("utf-8")).hexdigest(),
        "_op_type": op_type,
        "_index": index_name,
        "_type": es_type,
        "_source": _source,
    }
    return doc


def build_es_client(host="localhost", port=9200, timeout=30) -> Elasticsearch:
    es = Elasticsearch(
        hosts=[{"host": host, "port": port}],
        timeout=timeout,
        max_retries=3,
        retry_on_timeout=True,
    )
    es.cluster.health(wait_for_status="yellow", request_timeout=1)
    return es
