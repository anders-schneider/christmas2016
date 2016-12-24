# Copyright 2015 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START app]
import logging
from google.cloud import bigtable

from flask import Flask
from flask import request

from random import randint


app = Flask(__name__)

def bytes_to_int(bytes):
	return int(bytes.encode('hex'), 16)


@app.route('/')
def hello():
    """Return a friendly HTTP greeting."""

    first_word = request.args.get('first_word')

    table_id = 'bible_counts'
    project_id = 'christmas-2016'
    instance_id = 'corpus-counts'

    column_family_id = 'col_family'
    column_id = 'counts'

    client = bigtable.Client(project=project_id)
    instance = client.instance(instance_id)
    table = instance.table(table_id)

    num_words = 0
    num_sentences = 0
    response = first_word

    while num_sentences < 1:
	    start_key = first_word + ":"
	    end_key = first_word + ":zzz"
	    result = table.read_rows(start_key=start_key, end_key=end_key)
	    result.consume_all()

	    count_map = dict()
	    total = 0
	    end_is_option = False
	    for row_key, row in result.rows.items():
	    	value = bytes_to_int(row.cells[column_family_id][column_id][0].value)
	    	total += value
	    	key = row_key.split(":")[1]
	    	end_is_option = key == "."
	    	count_map[key] = value

	    if num_words > 15 and end_is_option:
	    	response += "."
	    	break

	    rand_index = randint(1, total)

	    for key, val in count_map.iteritems():
	    	rand_index -= val
	    	if rand_index <= 0:
	    		if key.isalpha():
		    		response += " " + key
		    	else:
		    		response += key
	    		num_words += 1
	    		print "found word #", num_words
	    		if key == ".":
	    			num_sentences += 1
	    		first_word = key
	    		break

    return response


@app.errorhandler(500)
def server_error(e):
    logging.exception('An error occurred during a request.')
    return """
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    """.format(e), 500


if __name__ == '__main__':
    # This is used when running locally. Gunicorn is used to run the
    # application on Google App Engine. See entrypoint in app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
# [END app]
