# arxiv-abstracts-elasticsearch

### setup elastic search
    docker-compose up -d
    
### minimal example to populate es-index
apdapt `host` in `populating_es_index.py` and run  [populating_es_minimal_example.py](populating_es_minimal_example.py)

### in some browser
1. goto

        http://<some_host>:5601/app/kibana#/dev_tools/console 

2. have fun with kibana console (formerly know as Sense)
    
    ![sample](images/sample_kibana_console.png)
    
### [aminer](https://www.aminer.org/oag2019) -data
  1. download data with [download_aiminer_data.py](download_aiminer_data.py)
  2. run [aminer_to_es.py](aminer_to_es.py)