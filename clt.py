from mrjob.job import MRJob
import numpy as np
import statistics as stat
from mrjob.step import MRStep

class MRAvgPairwiseLines(MRJob):
    
    # 1x Paarschlüssel erstellen und Timestap weitergeben
    # 1x Paarschlüssel und Timestamp von msg_snd weitergeben
    def input_mapper(self, _, line):
       column = line.split(',')
       if "time" not in line:
         yield column[1] + "/" + column[2], (int(column[6])) 
       if "msg_snd" in line:
         yield column[1] + "/" + column[2], ((int(column[6])//1000//60)*60)

    # 1x Nach Paarschlüssel die Differenz von Senden und Empfangen weitergeben,
    # sowie den neuen Schlüssel für den zweiten Mapper
    def input_reducer(self,key,values):
       values = list(values)
       if len(values) == 3:
         yield key, (values[2] - values[0], values[1])

    # Neuer Schlüssel setzen (pro Minute) und Differenzen
    def output_mapper(self,_,values):
       yield values[1],values[0]

    # Nach dem neuen Schlüssel (pro Minute) gruppieren und 
    # den Durchschnitt der Differenzen berechnen
    def output_reducer(self,key,values):
      yield key, stat.mean(values) 

    def steps(self):
       return [
               MRStep(mapper=self.input_mapper, reducer=self.input_reducer),
               MRStep(mapper=self.output_mapper, reducer=self.output_reducer)
              ]

if __name__ == '__main__':
     MRAvgPairwiseLines.run()
