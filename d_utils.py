# utilities.py


def parseFile(fileLoc, delimiter='|', seriesIn='c'):
	"""
		inputs:
		fileLoc: path to delimited data file
		delimiter: delimiter character used in datafile. Defaults to pipe delimited
		seriesIn: specifies if the data series is in rows or columns. Defaults to columns

	"""	
	
	try:
		fileHandle = open(fileLoc,'r')
	except IOError as e:
		return e


	if not isinstance(delimiter, basestring): delimiter='|'

	if not isinstance(seriesIn, basestring):
		seriesIn='c'
	else:
		seriesIn = seriesIn[0]
		if seriesIn.lower() not in ['c', 'r']: seriesIn = 'c'
	
	# lines = []
	
	# rotate for r
	if seriesIn == 'r':
		lines = [list(x) for x in (zip(*[l.split(delimiter) for l in fileHandle]))]

		
	else:
		lines = [l.split(delimiter) for l in fileHandle]
		

	fileHandle.close()
	
	for l in lines: l[-1]=l[-1].rstrip() #remove chomp
	
	dataAsDict = {
			lines[0][i]:[ # i is the heading
					lines[j][i] for j in xrange(1,len(lines))]  # j is the heading

					for i in xrange(0,len(lines[0]))
					}

	kz = list(enumerate(dataAsDict.keys()))
	col_idx = {k[0]:k[1] for k in kz}
	
	return dataAsDict, col_idx