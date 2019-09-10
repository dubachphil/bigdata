from mrjob.job import MRJob
import numpy as np
import statistics as stat

class mrWordCount(MRJob):


    def mapper(self, _, line):
       words = line.split(',')
       if "time" not in line: 
          yield words[1] + "/" + words[2], int(words[6])
       if "msg_snd" in line:
          yield words[1] + "/" + words[2], ((int(words[6])//1000)*60) 

    def reducer(self, key, values):
       values = list(values)
       response =  0
       if len(values) == 3:
        response = stat.mean(values[2]-values[0])
       yield values[1] , response

#         yield key, values[0]

#    def reducer(self, key, values):
#        yield key, count(values)



if __name__ == '__main__':
    mrWordCount.run()
