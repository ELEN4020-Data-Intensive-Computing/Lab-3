from mrjob.job import MRJob
from mrjob.step import MRStep
import os
import re
import sys
import time


class MatrixMultiplication(MRJob):
	def steps(self):
		return [MRStep(mapper = self.mapper, reducer = self.reducer1),MRStep( reducer = self.reducer2)]
	def mapper(self, _, line):
		
		if len(line.split()) >2:  
		
			x,y,v = line.split()	
			x = int(x)
			y = int(y)
			v = int(v)	 
		
			filename = os.environ['map_input_file'] #checks from which input the element is from

			if filename  == "outA2.list":
					yield y , ('Matrix1', x,v)
			else: 
					yield x, ('Matrix2', y, v)
				

	def reducer1(self, j, values):
		matrixA=[]
		matrixB=[]

		for v in values:
			if v[0] == 'Matrix1':
				matrixA.append(v)
			else:
				matrixB.append(v)

		for v1 in matrixA:
			for v2 in matrixB:	
				yield (v1[1],v2[1]), v1[2]*v2[2]
			

	def reducer2(self, key, values):
		yield key, sum(values)

 			

if __name__ == '__main__':
	matrixOut = open("MatrixOut.list","w")
	matrix1 = open("outA2.list","r")
	lineA = matrix1.readline()
	M_c,M_r = lineA.split()	
  
	matrix2 = open("outA2.list","r")
	lineB = matrix2.readline()
	N_c,N_r = lineB.split()	
	M_c = int(M_c)
	M_r = int(M_r)
	N_c = int(N_c)
	N_r = int(N_r)

	start = time.time()
	MatrixMultiplication.run()
	end = time.time()
	print(end-start)

