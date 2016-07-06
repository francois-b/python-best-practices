"""Index & Query Utility for the dummy "Supercharger" project.

This module provides indexing capabilities to easily send text to ES.


Usage:
  indexer --index
  indexer --query <text>
  indexer --version

Options:
  -h --help     Show this screen."""

import csv
import json

import elasticsearch


# Load the configuration
with open("config.json") as config_file:
	cfg = json.load(config_file)

es = elasticsearch.Elasticsearch()


def index(index_name=cfg["index_name"], type_name=cfg["type_name"]):
	"Send documents to Elasticsearch"

	# Import data from the CSV file
	with open("data.csv") as data_file:
		csvreader = csv.DictReader(data_file)

		# Turn the CSV data into a list of dictionaries
		documents = []
		for line in csvreader:
			documents.append(line)

	# If the index already exists, delete it
	if index_name in es.indices.stats()["indices"].keys():
		es.indices.delete(index=index_name)

	es.indices.create(index=index_name, body=cfg["index_metadata"])

	# Index all documents from the CSV file
	for doc in documents:
	    es.index(index=index_name, doc_type=type_name, body=doc)


def query(text, index_name=cfg["index_name"], type_name=cfg["type_name"]):
	"Issue a simple text query to Elasticsearch."

	body = {
	    "query": {
	        "fuzzy": {
	            "body": text
	        }
	    }
	}
	results = es.search(index=index_name, doc_type=type_name,
						body=body)
	return results["hits"]["hits"]


if __name__ == "__main__":

	from docopt import docopt
	opts = docopt(__doc__)

	if opts["--index"]:
		index()
	elif opts["--query"]:
		print query(opts["<text>"])
	else:
		print "Wrong invocation. See --help."
