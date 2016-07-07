import time

import elasticsearch
import pytest

import supercharger


def test_indexing():
    es = elasticsearch.Elasticsearch()
    supercharger.index(index_name="test-dummy-index")

    # Wait for Elasticsearch to index the documents
    time.sleep(1)

    # Query for all documents
    documents = es.search(index="test-dummy-index",
                          body={"query": {"match_all": {}}})["hits"]["hits"]

    # Make sure we get all documents sent
    assert len(documents) == 8

def test_query():
    results = supercharger.query(text="coffee", index_name="test-dummy-index")
    assert len(results) == 1
    assert results[0]["_source"]["category"] == "email"

def test_bad_query():
    with pytest.raises(supercharger.BadQueryError):
        results = supercharger.query(text='//"}}',
                                     index_name="test-dummy-index")

def test_es_index_not_found(capsys):
    # If the index already exists, delete it
    es = elasticsearch.Elasticsearch()
    if "test-dummy-index" in es.indices.stats()["indices"].keys():
        es.indices.delete(index="test-dummy-index")

    # Excecute a query on an index that doesn't exist
    results = supercharger.query(text="coffee",
                                 index_name="test-dummy-index")

    # Capture standard output and compare
    out, err = capsys.readouterr()
    assert out == "Elasticsearch index not found: test-dummy-index\n"
