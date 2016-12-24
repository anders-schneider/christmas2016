from google.cloud import bigtable
import re

filename = 'bible.txt'
table_id = 'bible_counts'
project_id = 'christmas-2016'
instance_id = 'corpus-counts'

def record(current, next, pairmap):
	key = current + ":" + next
	if key in pairmap:
		pairmap[key] = pairmap[key] + 1
	else:
		pairmap[key] = 1

# # The client must be created with admin=True because it will create a
# # table.
# client = bigtable.Client(project=project_id, admin=True)
# instance = client.instance(instance_id)

# print('Creating the {} table.'.format(table_id))
# table = instance.table(table_id)
# table.delete()
# table.create()
# column_family_id = 'col_family'
# column_id = 'counts'
# counts_col = table.column_family(column_family_id)
# counts_col.create()

with open(filename) as f:
	lines = f.readlines()

pairmap = dict()
prev = ''

for line in lines:
	for unit in line.split(' '):
		unit = unit.strip().lower()
		if unit is not '':
			if re.match(r'\)', unit[-1]):
				unit = unit[:-1]

			if re.match(r',|\.|;|:|\?|!|\)', unit[-1]):
				record(prev, unit[:-1], pairmap)
				record(unit[:-1], unit[-1], pairmap)
				prev = unit[-1]
			elif re.match(r'[0-9]+:[0-9]+', unit):
				pass
			elif prev is not '':
				record(prev, unit, pairmap)
				prev = unit
			else:
				prev = unit

for key, value in sorted(pairmap.iteritems()):
	print "Wrote:", key, "-->", value
	# row = table.row(key)
	# row.set_cell(column_family_id, column_id, value)
	# row.commit()