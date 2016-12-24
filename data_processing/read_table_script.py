from google.cloud import bigtable
import re
import pdb

def bytes_to_int(bytes):
	return int(bytes.encode('hex'), 16)

table_id = 'bible_counts'
project_id = 'christmas-2016'
instance_id = 'corpus-counts'

column_family_id = 'col_family'
column_id = 'counts'

client = bigtable.Client(project=project_id)
instance = client.instance(instance_id)

table = instance.table(table_id)

rows = table.read_rows()
rows.consume_all()

for row_key, row in rows.rows.items():
	value = bytes_to_int(row.cells[column_family_id][column_id][0].value)
	print "Read", row_key, "-->", value