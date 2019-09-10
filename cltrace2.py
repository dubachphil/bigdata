from mrjob.job import MRJob
import numpy as np
import statistics as stat
from mrjob.step import MRStep

class MRAvgPairwiseLines(MRJob):

    def input_mapper(self, _, line):
       column = line.split(',')
       if "time" not in line:
         yield column[1] + "/" + column[2], (int(column[6])) 
       if "msg_snd" in line:
         yield column[1] + "/" + column[2], ((int(column[6])//1000)*60)

    def input_reducer(self,_,values):
       values = list(values)
       if len(values) == 3:
         yield values[1], (values[2] - values[0]) #,key)

    def output_mapper(self,key,values):
       yield key, values

    def output_reducer(self,key,values):
      yield key, stat.mean(values) 

    def steps(self):
       return [
               MRStep(mapper=self.input_mapper, reducer=self.input_reducer),
               MRStep(mapper=self.output_mapper, reducer=self.output_reducer)
              ]

if __name__ == '__main__':
     MRAvgPairwiseLines.run()
