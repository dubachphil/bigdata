from mrjob.job import MRJob

class mrWordCount(MRJob):

    def mapper(self, _, line):
       if "res_snd" in line:
         columns = line.split(",")
         yield ((int(columns[6])//1000//60)*60) ,1

    def combine(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    mrWordCount.run()
