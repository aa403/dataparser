
__author__ = "Ammar Akhtar"
__version__ = "0.0.1"
__maintainer__ = "Ammar Akhtar"
__status__ = "Development"

"""
Created as an example extension to Data Loader
"""


import os
import DataLoader
from DataLoader import xsvParser

class ExampleLoader(DataLoader.DataLoader):

	def someheading(self, **file_data):

		derived_col = []

		for customer in file_data['CUSTOMER_ID_MAIN']:
			derived_col.append(str(customer))

		return derived_col




if __name__ == '__main__':

	# main(sys.argv[1:])
	# d_utils.parseFile()
	print os.path.dirname(os.path.realpath(__file__))
	a = ExampleLoader(os.path.dirname(os.path.realpath(__file__)),path_instr="default_path_instr.conf")
	print a.__dict__

	# print DataLoader.__doc__

