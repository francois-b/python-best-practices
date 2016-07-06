"""Indexer for the dummy "Supercharger" project.

This module provides indexing capabilities to easily send text to ES.
"""

import csv
import json

import elasticsearch

def index():
	"Send documents to Elasticsearch"

	# Import data from the CSV file
	csvreader = csv.DictReader(open("data.csv"))

	# Name of the index as a global variable
	INDEX_NAME = "supercharger"

	# Turn the CSV data into a list of dictionaries
	documents = []
	for line in csvreader:
		documents.append(line)

	es = elasticsearch.Elasticsearch()

	# If the index already exists, delete it
	if INDEX_NAME in es.indices.stats()["indices"].keys():
		es.indices.delete(index=INDEX_NAME)

	# Define the settings and mappings of our index
	index_settings_and_mappings = json.load(open("index_metadata.json"))
	es.indices.create(index=INDEX_NAME, body=index_settings_and_mappings)

	# Index all documents from the CSV file
	for doc in documents:
	    es.index(index=INDEX_NAME, doc_type="communication_item", body=doc)

index()