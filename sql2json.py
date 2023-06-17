import os, sys, json, re, threading, time, queue, argparse, gzip
import phpserialize as php_s
from sql_metadata import Parser

# how long to keep checking queue
queue_timeout = 10

# how many records to queue up before flushing to file
flush_batch_size = 10000

# existing output files
seen_files = []

# worker threads
threads = {}

# table statement queues
queues = {}

# json output directory
output_dir = os.path.dirname(os.path.realpath(__file__))

# tables to convert
tables = None

# column value parsers
parsers = None

# input sql file path
sql_file_path = None


# parse json config
def parse_config(config):
	global tables, parsers, output_dir, flush_batch_size, queue_timeout

	if 'tables' in config:
		tables = config['tables']

	if 'parsers' in config:
		parsers = config['parsers']

	if 'output_dir' in config:
		output_dir = config['output_dir']

	if 'flush_batch_size' in config:
		flush_batch_size = config['flush_batch_size']

	if 'queue_timeout' in config:
		queue_timeout = config['queue_timeout']

# parse command line arguments
def parse_args():
	global sql_file_path, output_dir, tables, flush_batch_size, queue_timeout

	args_parser = argparse.ArgumentParser(prog='sql2json', description='Convert sql dump to json')
	args_parser.add_argument('--config', help='Config file path', default=None)
	args_parser.add_argument('--output_dir', help='Output directory', default=None)
	args_parser.add_argument('--tables', help='Comma separated list of tables to export', default=None)
	args_parser.add_argument('--flush_batch_size', help='minimum number of records to queue up before flushing to file', default=None, type=int)
	args_parser.add_argument('--queue_timeout', help='maximum time to wait for new statement to be available', default=None, type=int)
	args_parser.add_argument('sql_file_path', help='SQL file path')

	args = args_parser.parse_args()

	sql_file_path = args.sql_file_path

	if args.config:
		if not os.access(args.config, os.R_OK):
			print('Config file not found or not readable')
			sys.exit(1)

		config = json.load(open(args.config))
		parse_config(config)

	if args.output_dir:
		output_dir = args.output_dir

	if args.tables:
		tables = args.tables.split(',')

	if args.flush_batch_size:
		flush_batch_size = args.flush_batch_size

	if args.queue_timeout:
		queue_timeout = args.queue_timeout

# get existing queue or create new one for table
def get_table_queue(table):
	if table in queues:
		q = queues[table]
	else:
		q = queue.Queue()
		queues[table] = q

	return q

# check if table should be converted
def match_table(table):
	if tables is None:
		return True

	for pattern in tables:
		if re.match(pattern, table):
			return True

	return False

# parse sql statement
def parse_statement(insert_sql):
	return Parser(insert_sql)

# chunk a list
def chunk(l, size):
	for i in range(0, len(l), size):
		yield l[i:i + size]

# decode json value
def decode_json(value):
	try:
		value = json.loads(value)
	except Exception as e:
		#If this is the error for slash-escaped json
		if str(e) == "Expecting property name enclosed in double quotes: line 1 column 3 (char 2)":
			try:
				#Try decoding it and removing backslashes
				decoded_value = value.encode('latin-1', 'backslashreplace').decode('unicode-escape')
				value = json.loads(decoded_value)
			except:
				pass

	return value

# convert php serialized child value
def convert_meta_value_item(data):
	#Convert from phpserialized based on type
	if isinstance(data, bytes):  return data.decode('utf-8')
	if isinstance(data, dict):   return dict(map(convert_meta_value_item, data.items()))
	if isinstance(data, tuple):  return map(convert_meta_value_item, data)
	return data

# decode php serialized value
def decode_php_s(value):
	try:
		#Unserialize it
		meta_value=php_s.unserialize(bytes(value, "utf-8"))
		meta_dict={}
		#Decode unserialized data
		for k,v in meta_value.items():
			key=convert_meta_value_item(k)
			val=convert_meta_value_item(v)
			meta_dict[key]=val

		return meta_dict
	except:
		return value

# get column value parsers for table
def get_table_parsers(table):
	if parsers:
		for pattern in list(parsers.keys()):
			if re.match(pattern, table):
				return parsers

	return None

# get column value parser config
def get_parser_config(parser):
	if parser == 'json':
		return {
			'pattern': r'^\{|\[',
			'function': 'decode_json'
		}
	elif parser == 'phps':
		return {
			'pattern': r'^a:\d+',
			'function': 'decode_php_s'
		}

	return None

# convert sql insert values to dict records
def parse_records(table, columns, value_groups):
	parsers = get_table_parsers(table)
	columns_count = len(columns)
	records = []
	for vg in value_groups:
		record = {}
		for i in range(columns_count):
			column_name = columns[i]
			column_value = vg[i]

			if parsers and column_name in parsers and isinstance(column_value, str):
				column_parsers = parsers[column_name]
				for parser in column_parsers:
					parser_config = get_parser_config(parser)
					if re.match(parser_config['pattern'], column_value):
						parser_function = parser_config['function']
						try:
							column_value = globals()[parser_function](column_value)
						except:
							pass

			record[column_name] = column_value

		records.append(record)

	return records

# write json records to file
def write_json(table, records):
	if len(records) == 0:
		return

	filename = table + '.json'

	# append to existing file
	if filename in seen_files:
		mode = 'a'
	# create new file
	else:
		mode = 'w'
		seen_files.append(filename)

	try:
		filepath = os.path.join(output_dir, filename)

		with open(filepath, mode) as f:
			json.dump(records, f)

		print("["+table+"] wrote "+str(len(records))+" records")
	except OSError:
		print("Failed to write file "+filepath)
		sys.exit(1)

def prepare_output_dir():
	if os.path.exists(output_dir) and os.path.isdir(output_dir):
		if not os.access(output_dir, os.W_OK):
			print("Output directory "+output_dir+" is not writable")
			sys.exit(1)
	else:
		try:
			os.makedirs(output_dir)
		except OSError:
			print("Failed to create output directory "+output_dir)

# Worker thread used to asynchronously process records
class Worker(threading.Thread):
	def __init__(self, table, queue):
		threading.Thread.__init__(self, args=(), kwargs=None)
		self.table = table
		self.queue = queue
		self.write_queue = []
		self.last_active = time.time()

	def run(self):
		print("[" + self.table + "] starting...")
		while True:
			try:
				# flush write queue if it's full
				if len(self.write_queue) >= flush_batch_size:
					write_json(self.table, self.write_queue)
					self.write_queue = []

				# wait for new statement to be available
				statement = self.queue.get(True, queue_timeout)
				columns = statement.columns
				value_groups = chunk(statement.values, len(columns))

				# append processed records to write queue
				self.write_queue = self.write_queue + parse_records(self.table, columns, value_groups)
				self.queue.task_done()
				self.last_active = time.time()
			except queue.Empty:
				break

		# flush remaining records
		write_json(self.table, self.write_queue)
		self.write_queue = []

		print("[" + self.table + "] done")



# get existing thread or create new one for table
def get_table_thread(table, q):
	if table in threads:
		thread = threads[table]
		if not thread.is_alive():
			thread = Worker(table, q)
			threads[table] = thread
	else:
		thread = Worker(table, q)
		threads[table] = thread

	return thread

def process_line(line):
	# only parse INSERT statements
	if line.startswith('INSERT INTO'):
		statement = parse_statement(line.strip())
		table = statement.tables[0]

		if not match_table(table):
			return

		q = get_table_queue(table)
		thread = get_table_thread(table, q)

		if not thread.is_alive():
			thread.start()

		# add parsed statement to queue to be processed by the worker thread for that table
		q.put(statement)

# process sql file line by line
def parse_sql_file(filename):
	if re.match(r'.*\.gz$', filename):
		with gzip.open(filename, 'rt') as f:
			for line in f:
				process_line(line)
	else:
		with open(filename, 'rt') as f:
			for line in f:
				process_line(line)

	print("Waiting for threads to finish...")



if __name__ == '__main__':
	parse_args()

	start_time = time.time()

	print("Parsing " + sql_file_path + "...")

	if not os.access(sql_file_path, os.R_OK):
		print("File " + sql_file_path + " is not readable")
		sys.exit(1)

	prepare_output_dir()

	parse_sql_file(sql_file_path)

	end_time = time.time()

	print("Finished in " + str(end_time - start_time) + " seconds")