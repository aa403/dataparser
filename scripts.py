# scripts.py
import sys, getopt
import d_utils

def main(*argv):
	"""
	ammar: TODO
	"""

    try:
        opts, args = getopt.getopt(argv, "hg:d", ["help", "grammar="])
    except getopt.GetoptError:

        sys.exit(2)
    

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print "usage()"
            sys.exit()                  
        elif opt == '-d':
            global _debug               
            _debug = 1                  
        elif opt in ("-g", "--grammar"):
            pass





if __name__ == '__main__':

	main(sys.argv[1:])
	d_utils.parseFile()
