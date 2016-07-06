import csv

import elasticsearch

csvreader = csv.DictReader(open("data.csv"))
INDEX_NAME = "supercharger"

documents = []
for line in csvreader:
	documents.append(line)

es = elasticsearch.Elasticsearch()

if INDEX_NAME in es.indices.stats()["indices"].keys():
	es.indices.delete(index=INDEX_NAME)

index_settings_and_mappings = {
    "settings": {
        "index": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "analysis": {
            "analyzer": {
                "multiple_word_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["multiple_word_filter"]
                },
                "complex_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                    	"lowercase", "stemmer", "my_synonym_filter", "stop",
                    	"multiple_word_filter", "positional_fix", "trim",
                    	"unique"
                    ]
                },
                "complex_query_analyzer": {
                    "type": "custom",
                    "tokenizer": "keyword",
                    "filter": ["multiple_word_filter"]
                }
            },
            "filter": {
                "multiple_word_filter": {
                    "type": "shingle",
                    "output_unigrams": True,
                    "min_shingle_size": 2,
                    "max_shingle_size": 10
                },
                "my_synonym_filter" : {
                    "type" : "synonym",
                    "synonyms" : [
                        "ze => the",
                        "all => the"
                    ]
                },
                "positional_fix": {
                    "type": "pattern_replace",
                    "pattern": " ?_",
                    "replace": ""
                }
            }
        }
    },
    "mappings": {
        "communication_item": {
            "properties": {
                "category": {
                    "type": "string",
                    "analyzer": "multiple_word_analyzer",
                    "search_analyzer": "keyword"
                },
                "body": {
                    "type": "string",
                    "analyzer": "complex_analyzer",
                    "search_analyzer": "complex_query_analyzer"
                }
            }
        }
    }
}

es.indices.create(index=INDEX_NAME, body=index_settings_and_mappings)

for doc in documents:
    es.index(index=INDEX_NAME, doc_type="communication_item", body=doc)


