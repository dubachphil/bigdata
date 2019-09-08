from mrjob.job import MRJob

class mrWordCount(MRJob):

    def mapper(self, _, line):
       if "res_snd" in line:
         columns = line.split(",")    
         yield "time", ((int(columns[6])//1000)*60)

    def reducer(self, key, values):
        yield key, count(values)

if __name__ == '__main__':
    mrWordCount.run()
