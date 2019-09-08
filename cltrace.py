from mrjob.job import MRJob
import numpy as np
import statistics as stat

class mrWordCount(MRJob):


    def mapper(self, _, line):
       if "time" not in line: 
          words = line.split(',')
          yield words[1] + "/" + words[2],int(words[6])

    def reducer(self, key, values):
       
       yield key, np.diff(values)

if __name__ == '__main__':
    mrWordCount.run()
