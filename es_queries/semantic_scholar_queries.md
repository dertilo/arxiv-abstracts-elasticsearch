# some ES-queries on index containing [Semantic Scholar Open Research Corpus](https://api.semanticscholar.org/corpus/)
### in kibana console

#### number of papers -> 177448124 # September 2019
    GET s2papers/_count
    {
      "query": {
        "match_all": {}
      }
    }

#### number of papers with abstract -> 97922977, thats ~100 mio. papers (September 2019)

    GET s2papers/_count
    {
      "query": {
        "exists": {
          "field": "paperAbstract"
        }
      }
    }

#### papers authored by Vinicius Woloszyn

    GET s2papers/_search
    {
      "query": {
        "match_phrase": {
          "authors.name": "Vinicius Woloszyn"
        }
      }
    }
    
#### some search

    GET s2papers/_search
    {
      "query": {
        "match": {
          "paperAbstract": "adversarial events hospital"
        }
      },
      "size": 20
    }