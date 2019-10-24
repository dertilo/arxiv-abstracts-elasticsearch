import json
from typing import List, Dict

from elasticsearch import Elasticsearch

from es_queries.match_phrase_query import get_paper_by_exact_match_authors_name
from es_util import build_es_client


def multisearch(es_client: Elasticsearch, search_bodies, index, doc_type):
    search_header = {"index": index, "type": doc_type}
    request = "".join(
        [
            "%s \n" % json.dumps(each)
            for search_body in search_bodies
            for each in [search_header, search_body]
        ]
    )
    out = es_client.msearch(body=request, index=index)
    return out


def multisearch_query(
    queries: List[Dict],
    es_client: Elasticsearch,
    index="test",
    doc_type="doc",
    includes=None,
    excludes=None,
):
    d = {}
    if includes is not None:
        d["includes"] = includes
    if excludes is not None:
        d["excludes"] = excludes

    search_arr = [{"query": q, "_source": d} for q in queries]

    out = multisearch(es_client, search_arr, index, doc_type)
    g = (
        o.get("hits", {}).get("hits", [{}])[0].get("_source", {})
        for o in out["responses"]
    )
    for hit in g:
        yield hit


if __name__ == "__main__":
    INDEX_NAME = "s2papers"
    TYPE = "paper"
    host = "gunther"
    authors_name = "Vinicius Woloszyn"
    es_client = build_es_client(host)
    limit = 2
    papers = get_paper_by_exact_match_authors_name(
        INDEX_NAME, authors_name, limit, es_client=es_client
    )
    papers_ids = papers[1]["outCitations"]

    queries = [{"match": {"id": eid}} for eid in papers_ids]

    for hit in multisearch_query(
        queries, es_client, INDEX_NAME, TYPE, includes="title"
    ):
        print(hit)
