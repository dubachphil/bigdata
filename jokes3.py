from mrjob.job import MRJob
import statistics as stat

class MR_jokes(MRJob):

    def mapper(self, _, line):
        words = line.split()

        if len(words) >= 11:
            yield "joke_01", float(words[1])
            yield "joke_02", float(words[2])
            yield "joke_03", float(words[3])
            yield "joke_04", float(words[4])
            yield "joke_05", float(words[5])
            yield "joke_06", float(words[6])
            yield "joke_07", float(words[7])
            yield "joke_08", float(words[8])
            yield "joke_09", float(words[9])
            yield "joke_10", float(words[10])

    def combine(self, joke, values):
        yield joke, round(stat.mean(values),2)

    def reducer(self, joke, values):
        yield joke, round(stat.mean(values),2)

if __name__ == '__main__':
    MR_jokes.run()
