"""Index & Query Utility for the dummy "Supercharger" project.

This module provides indexing capabilities to easily send text to ES.


Usage:
  supercharger --index
  supercharger --query <text>
  supercharger --web
  supercharger --version

Options:
  -h --help     Show this screen."""

import csv
import json
import sys

import elasticsearch
from jinja2 import Template

from flask import Flask
app = Flask(__name__)


# Load the configuration
with open("config.json") as config_file:
	try:
		cfg = json.load(config_file)
	except ValueError:
		print "The configuration file is not valid JSON."
		sys.exit()

es = elasticsearch.Elasticsearch()

class BadQueryError(Exception):
	pass

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

	template = Template(json.dumps(cfg["query_template"]))
	body_string = template.render(query_text=text)
	try:
		body = json.loads(body_string)
	except ValueError:
		raise BadQueryError(
			"The query string should only contain alpha-numeric text")

	try:
		results = es.search(index=index_name, doc_type=type_name,
							body=body)
	except elasticsearch.exceptions.NotFoundError:
		print "Elasticsearch index not found:", index_name
		return

	return results["hits"]["hits"]


@app.route("/")
def hello():
	return "Hello World!"


@app.route('/query/<text>')
def web_query(text):
	results = query(text)
	output = "<br><br>".join([item["_source"]["body"] for item in results])
	return "<h2>Results for '{0}':</h2><br>{1}".format(text, output)


if __name__ == "__main__":

	from docopt import docopt
	opts = docopt(__doc__)

	if opts["--index"]:
		index()
	elif opts["--query"]:
		results = query(opts["<text>"])
		if results:
			print results
	elif opts["--web"]:
		app.run()
	else:
		print "Wrong invocation. See --help."
