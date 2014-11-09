
__author__ = "Ammar Akhtar"
__version__ = "0.0.1"
__maintainer__ = "Ammar Akhtar"
__status__ = "Development"

import sys, getopt, os
import logging
import inspect
import references


class DataLoader(object):
	"""
	Loads a set of delimited files to a provided path, based on instructions provided.

	can be used as-is or extended to add derivation rules - see ExampleLoader.py
	

	Args:
		path (str): path to folder containing data
		
		path_instr (str): pipedelimited load instructions for this folder


	Attributes created:
		data_path (str): path to folder containing delimited data files
		
		path_instr (str): file containing pipe delimited load instructions for this folder; of the form:
			provider|string<name>
			dataset|int
			write_path|string<path>

		load_instr(dict): dictionary containing load instructions read from path_instr

		write_path(str): path to output files to read from load_instr
		
		content_seq_counter(dict): dictionary to count loads made of each content type

		file_instr_path(str): path to where all file instructions are held
		
		file_context(str or None): initialises file_context as None, used to set context in filename of a file to be loaded
		
		file_content_type(str or None): initialises file_content_type as None, used to set file extension based on content a file to be loaded
		
		files_created(list of str): empty list to store created files
		
		out_file_name: empty string output filename initialised

		ref_data_path (str):  path to where reference data is held

		ref_data (dict): dictionary containing reference data required for class to operate

		warnings (str): set of warnings recorded on this class instance
		
		errors (str): set of errors recorded on this class instance
	

	"""	

	# TODO: set for local machine. 
	DEFAULT_PATH = os.path.dirname(os.path.realpath(__file__))

	def log_error(self, error):
		""" Add an error to the self.errors string to file or return later """
		if isinstance(error, basestring):
			self.errors += error + '|'
			logging.error('Error from handler %s: %s' % (inspect.stack()[1][3], error))
			

	def log_warning(self, warning):
		""" Add a warning to the self.warnings string to file or return later """
		if isinstance(warning, basestring):
			self.warnings += warning + '|'
			logging.warning('Warning from handler %s: %s' % (inspect.stack()[1][3], warning))
			

	def clear_errors(self):
		""" Reset errors """
		self.warnings = ''
		self.errors = ''
		# self.results.update({'warnings': ''})
		# self.results.update({'errors': ''})

	def __init__(self, data_path, path_instr=None):
		super(DataLoader, self).__init__()

		logging.basicConfig(level=logging.INFO,
							filename='log.log', # log to this file
							format='%(asctime)s %(message)s') # include timestamp

		self.errors = '' # log errors in pipe delimited string using the log_error() method
		self.warnings = '' # log warnings  in pipe delimited string using the log_warning() method

		# set file paths
		if not os.path.isdir(data_path):
			self.log_warning("data_path must be valid path, setting path to %s"%self.DEFAULT_PATH)
			self.data_path = self.DEFAULT_PATH
		else:
			self.data_path = os.path.realpath(data_path)


		if not os.path.isfile(path_instr):
			self.log_warning("invalid path instructions provided, using defaults in default_path_instr.conf")
			self.path_instr = "default_path_instr.conf"
		else:
			self.path_instr = os.path.realpath(path_instr)

		# open path instructions file
		self.load_instr = xsvParser(self.path_instr, delimiter='|', seriesIn='r')
		
		if os.path.isdir(self.load_instr['write_path']):
			self.write_path = os.path.realpath(self.load_instr['write_path'])
		else:
			
			self.log_warning("invalid write_path: %s , setting path to %s"%(self.load_instr['write_path'],self.DEFAULT_PATH))

			self.write_path = self.DEFAULT_PATH
		
		self.content_seq_counter = {}
		self.file_instr_path = None
		self.file_context = None
		self.file_content_type = None
		self.files_created = []
		self.out_file_name = ""

		if not isinstance(self.load_instr['ref_data_path'], basestring):
			self.log_warning("ref_data_path must be string, setting path to %s"%self.DEFAULT_PATH)
			self.ref_data_path = self.DEFAULT_PATH
		else:
			self.ref_data_path = os.path.realpath(self.load_instr['ref_data_path'])

		self.ref_data = {}

	def set_file_instr_path(self, file_instr_path):

		if os.path.isdir(file_instr_path):
			self.file_instr_path = os.path.realpath(file_instr_path)
		else:
			self.log_error("invalid file_instr_path: %s" % file_instr_path)
			self.file_instr_path = None


	def execute(self):
		"""
		Executes data loads for each set of file instructions
		"""
		if self.file_instr_path is None:
			self.log_error("cannot execute - set up a file_instr_path using set_up() method")
			return False

		import glob

		for fconf in glob.iglob(os.path.join(self.file_instr_path, "*.fconf")):
			r = self.run_loader(fconf)
			if r is not True: self.log_error("%s - %s" % str(fconf), str(r))

	def run_loader(self, file_instr):
		"""
		Runs the data load routine for a file

		returns true if file loaded correctly, or Exception otherwise
		"""

		if not os.path.isfile(file_instr):
			self.log_warning("invalid path instructions provided, using defaults in default_path_instr.conf")
			self.file_instr = "default_path_instr.conf"
		else:
			self.file_instr = file_instr

		# load instructions for this file
		self.set_file_instr()

		# parse file into file_data
		file_data = xsvParser(os.path.join(self.data_path,self.source_file))

		self.set_out_file_name()

		write_file = os.path.join(self.write_path,self.out_file_name)
		# write_file = os.path.realpath(os.path.join(os.path.dirname(self.write_path,self.out_file_name)))
		#todo: need to add some sort of a limitter to stop the wrtie file getting too big; also need a way of splitting by context

		# implicit that there must be some load_cols
		if self.file_load_cols == '':
			self.log_warning("no load_cols required for %s" % self.source_file)

			return None

		elif self.file_load_cols == '__!all__': # write the file as-is to self.write_path
			l_headings = sorted(file_data.keys(), key=str.lower)

			# get all columns from dict and build rows using zip
			# THIS IS THE VARIABLE TO KNOW WHETHER TO SPLIT: self.file_split_col

			rows_to_write = zip(*[file_data[h] for h in l_headings ])

			# nothing further to do
			return write_pipe_delimited(write_file, rows_to_write, l_headings)


		else:  # find the columns to write
			# identify cols to load
			l_headings = [h.strip().replace(" ", "_") for h in self.file_load_cols.split(",")]

			# identify cols to derive and create placeholder in derived_cols
			d_headings = self.file_derive_cols.split(",")
			derived_cols = []

			# get all derived data
			for dh in enumerate(d_headings, start=1):
				dv = getattr(self, dh[1], None)

				if dv is None:
					self.log_warning("no derivation rule for %s in %s" %(dh[1], self.source_file))
				else:

					derived_cols.append(
						dv(**dict(file_data.items() + zip(d_headings[:dh[0]],derived_cols)))
					)

			if self.file_split_col in ['', ' ', None, False]:
				# get appropriate columns from dict and build rows using zip*
				rows_to_write = zip(*
									[file_data[h] for h in l_headings] + derived_cols
								)
				
				return write_pipe_delimited(write_file, rows_to_write, l_headings + d_headings)

			else:
				success = True
				try:
					split_set = list(set(file_data[self.file_split_col]))
				except:
					split_set_index = d_headings.index(self.file_split_col)
					split_set = list(set(derived_cols[split_set_index]))
				all_rows = zip(*
									[file_data[h] for h in l_headings] + derived_cols
								)

				for item in split_set:
					index_of_col = (l_headings + d_headings).index(self.file_split_col)
					rows_to_write = [row for row in all_rows if row[index_of_col] == item]
					filenamebase = write_file.lower().split('.')
					itersuccess = write_pipe_delimited(filenamebase[0][:-4] + str(item) + '.' + filenamebase[1], rows_to_write, l_headings + d_headings)
					success = success and itersuccess

				return success





	def get_reference_data(self):
		"""
		Loads reference data from .ref pipe delimited files which should be in
		the same folder as the execution
		"""
		import glob

		for data in glob.iglob(os.path.join(self.ref_data_path, "*.data")):
			new_ref_data = xsvParser(data)
			ref_data={}

			if new_ref_data is IOError:
				self.log_warning("reference data %s not loaded" % data)
				continue

			for k in new_ref_data:

				if k in ref_data:
					ref_data[k]+=new_ref_data[k]

				else:
					ref_data.update({k:new_ref_data[k]})

			self.ref_data.update({data.split('\\')[-1][:-5]:ref_data})

	def set_out_file_name(self, context_val=None):
		"""
		builds filename as <provider>_[dataset_num]_<<context__optional>>_[seq_num].<<content>>

		where each:
			<value> is taken from load_instr, and 
			<<value>> is taken from fil_instr, and 
			[value] is generated from the respective get_value_num() method

		examples:
			provider_001_0001.cust
			provider_002_user_0005.trans
			provider_002_user12345678_0005.trans
			provider_003_0129.dat

		"""
		self.out_file_name = self.load_instr['provider'] + '_' + self.get_dataset_num()
		
		if self.file_context is not None: 
			self.out_file_name += "_"+self.file_context

		if context_val is not None:
			self.out_file_name += "_"+context_val

		self.out_file_name += "_"+self.get_seq_num()
		self.out_file_name += "."+self.file_content_type


	def clear_out_file_name(self):
		self.out_file_name = ""


	def set_file_instr(self):
		"""
		set read instructions for data file being processed

		source_file: string filepath

		file_context: 
			optional context on the file - used to set filename and partitioning. 
			eg if value is 'user' then the data will be split by user

		file_content_type: 
			string to denote content of the file, used to set file extension
			eg 'cust' or 'trans' for customer or transaction data respectively

		file_load_cols:
			comma seperated string identifying the cols to load as-is from source_file. 
			OR __!all__ for everything

		file_derive_cols:
			comma seperated string identifying the cols to load via 
			derivation rule from source_file. OR __!all__ for all columns. 
			NOTE:
				derivation rules must be set for each column in 
				the derive_<source>_<col_name>() method on the class, else the column will be ignored

		"""
		self.f_instr = xsvParser(self.file_instr, delimiter='|', seriesIn='r')
		
		self.source_file = self.f_instr['source']
		self.file_context = self.f_instr.get('context', None)
		self.file_content_type = self.f_instr['content']
		self.file_load_cols = self.f_instr['load_cols']
		self.file_split_col = self.f_instr.get('split_on', None)

		if len(self.file_load_cols) > 1:
			while self.file_load_cols[-1] == ",":
				self.file_load_cols = self.file_load_cols[:-1]

		self.file_derive_cols = self.f_instr['derive_cols']
		if len(self.file_derive_cols) > 1:
			while self.file_derive_cols[-1] == ",":
				self.file_derive_cols  = self.file_derive_cols[:-1]


	def get_seq_num(self):
		"""
		returns the batch number for each content type in this path
		output: "####"

		"""

		if self.file_content_type in self.content_seq_counter:
			self.content_seq_counter[self.file_content_type] += 1
		else:
			self.content_seq_counter.update({self.file_content_type:1})

		ctr_as_str = str(self.content_seq_counter[self.file_content_type])

		return ((4 - len(ctr_as_str)) * '0') + ctr_as_str
	
	def get_dataset_num(self):
		"""
		converts the dataset in the path_instr file to string of len 3
		output: "###"
		"""
		return ((3 - len(str(self.load_instr['dataset']))) * '0') + str(self.load_instr['dataset'])
	

def xsvParser(fileLoc, delimiter='|', seriesIn='c', single_series_flat=True):
	"""
	loads delimited files to dictionaries
	
	args:
		fileLoc(str): full path to delimited data file - include the file itself

		delimiter(str): delimiter character used in datafile. Defaults to pipe

		seriesIn(str): specifies if data series are in rows or columns. Defaults to columns

		single_series_flat: if there is only one entry for ALL series, then the response will not be wrapped in a list.


	Output:
		single_series_flat == false: dataAsDict(dict) = { row or col header : [ series in row or col ] }

		single_series_flat == true: dataAsDict(dict) = { row or col header : value in row or col }


	"""	
	
	try:
		fileHandle = open(fileLoc,'r')
	except IOError as e:
		return e

	# print "FILE", fileLoc
	if not isinstance(delimiter, basestring): delimiter='|'

	if not isinstance(seriesIn, basestring):
		seriesIn='c'
	else:
		seriesIn = seriesIn[0]
		if seriesIn.lower() not in ['c', 'r']: seriesIn = 'c'
	
	# rotate for r
	if seriesIn == 'r':
		lines = [list(x) for x in (zip(*[l.split(delimiter) for l in fileHandle]))]
		lines[-1] = [l.rstrip() for l in lines[-1]] #remove chomp
		
	else:
		lines = [l.split(delimiter) for l in fileHandle]
		for l in lines: l[-1]=l[-1].rstrip() #remove chomp

	fileHandle.close()
	
	
	dataAsDict = {
			lines[0][i]:[ # i is the heading
					lines[j][i] for j in xrange(1,len(lines))]  # j is the heading

					for i in xrange(0,len(lines[0]))
					}
		
	if all(len(dataAsDict[k]) == 1 for k in dataAsDict) and single_series_flat==True:
		for k in dataAsDict:dataAsDict[k] = dataAsDict[k][0]
	

	return dataAsDict

def write_pipe_delimited(write_file, rows_to_write, headings=None):
	import csv

	try:
		out_file = csv.writer(open(write_file, 'wb'), delimiter="|")

		if headings is not None:
			out_file.writerow(headings)

		out_file.writerows(rows_to_write)

		return True

	except Exception as e:
		return e

def dict_headers(dataAsDict):
	kz = list(enumerate(dataAsDict.keys()))
	return {k[0]:k[1] for k in kz}



if __name__ == '__main__':

	# main(sys.argv[1:])
	# d_utils.parseFile()
	print os.path.dirname(os.path.realpath(__file__))
	a = DataLoader(os.path.dirname(os.path.realpath(__file__)),path_instr='wave2_path_instr.conf')
	print a.__dict__

	# print DataLoader.__doc__



	print
	print
	fileLoc = "C:/Users/aakh/Documents/python/rows.csv"
	print fileLoc
	print xsvParser(fileLoc,delimiter=',', seriesIn='r')
	print
	print
	fileLoc = "C:/Users/aakh/Documents/python/cols.csv"
	print fileLoc
	print xsvParser(fileLoc,delimiter=',', seriesIn='c')
	print
	print
	print xsvParser("default_file_instr.fconf", delimiter='|', seriesIn='r')