# arxiv-abstracts-elasticsearch

### setup
    docker-compose up -d
    
### populate es-index
apdapt `host` in `populating_es_index.py` and run  

    python populating_es_index.py

### in some browser
1. goto

        http://<some_host>:5601/app/kibana#/dev_tools/console 

2. have fun with kibana console (formerly know as Sense)
    
    ![sample](images/sample_kibana_console.png)