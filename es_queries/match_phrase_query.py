from pprint import pprint

from es_util import build_es_client


def get_paper_by_exact_match_authors_name(INDEX_NAME,authors_name,limit,host='localhost'):
    body = \
        {
            "query": {
                "match_phrase": {
                    "authors.name": authors_name
                }
            },
            "size": limit
        }
    es = build_es_client(host)
    hits = es.search(index=INDEX_NAME, body=body)['hits']['hits']
    papers = [hit['_source'] for hit in hits]
    return papers


if __name__ == '__main__':
    INDEX_NAME = "s2papers"
    TYPE = "paper"
    host = 'gunther'
    authors_name = "Vinicius Woloszyn"
    limit = 20
    papers = get_paper_by_exact_match_authors_name(INDEX_NAME,authors_name,limit,host)
    pprint([p['title'] for p in papers])

