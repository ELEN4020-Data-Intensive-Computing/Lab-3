from mrjob.job import MRJob
from mrjob.step import MRStep
import os
import re
import sys
import time


class MatrixMultiplication(MRJob):
	def steps(self):
		return [MRStep(mapper = self.mapper, reducer = self.combiner)]
	def mapper(self, _, line):
		
		if len(line.split()) >2:  
		
			x,y,v = line.split()	
			x = int(x)
			y = int(y)
			v = int(v)	 
		
			filename = os.environ['map_input_file'] #checks from which input the element is from

			if filename  == "Matrix1.list":
				for col1 in range(0,N_c):
					yield (x,col1) , ('Matrix1', y,v)
			else: 
				for row1 in range(0,M_c):
					yield (row1, y), ('Matrix2', x, v)
					#print [((row1, y), ('Matrix2', x, v)), file = MatrixOut]
					matrixOut.write("%s %s %s %s %s\n" % (str(row1) ,str(y),'Matrix2', str(x), str(v)))
				

	def combiner(self, key, values):
		matrixA=[0]*N_r
		matrixB=[0]*N_r
		#file = open("output.list","w")
		for v in values:
			if v[0] == 'Matrix1':
				matrixA[v[1]] = v[2]
			else:
				matrixB[v[1]] = v[2]
		matrixC = []
		for v1 in range(0,M_c):
			result = matrixA[v1]*matrixB[v1]
			matrixC.append(result)			
		yield key, sum(matrixC)

 		
		

if __name__ == '__main__':
	matrixOut = open("MatrixOut.list","w")
	matrix1 = open("outA1.list","r")
	lineA = matrix1.readline()
	M_c,M_r = lineA.split()	
  
	matrix2 = open("outB1.list","r")
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

