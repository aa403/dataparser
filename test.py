

class testing(object):

	def __init__(self, arg1, arg2):
		print "arg1 is ", arg1
		print "arg2 is ", arg2

	# def my_func(arg1, arg2):
	# 	print "arg1 is ", arg1
	# 	print "arg2 is ", arg2

	# 	return 0


if __name__ == '__main__':

    import argparse

    parser = argparse.ArgumentParser(description='a test for passing arguments')

    parser.add_argument('--arg1', required=True,
                        help = 'word1')

    parser.add_argument('--arg2', required=False,
                        help = 'word2')

    args = parser.parse_args()

    print args

    # print *args

    # print **args

    # print (**args.__dict__)

    testing(**args.__dict__)

