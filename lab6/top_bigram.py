from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("WordCount").setMaster("local")

sc = SparkContext(conf=conf)

textFile = sc.textFile("../../bible+shakes.nopunc")

def split(line): 
    words = line.split(" ")
    bigrams = []
    for i in range(0,len(words)-1):
        bigrams.append(words[i] + " " + words[i+1])
    return bigrams
    
def generateone(word): 
    return (word, 1)

def sum(a, b):
    return a + b

def split_bigram(bigram):
    words = bigram[0].split(" ")
    left = words[0]
    right = words[1]
    return [(left, bigram), (right, bigram)]

def extract_top(big_group):
    top = [None] * 5
    max_val = 0
    max_word = tuple()
    for i in range(0,5):
        for big in big_group[1]:
            if big[1] > max_val and (big not in top):
                max_word = big
                max_val = big[1]
        top[i] = max_word
        max_val = 0
    return (big_group[0], top)

bigrams = textFile.flatMap(split).map(generateone).reduceByKey(sum)
bigrams = bigrams.flatMap(split_bigram).groupByKey().map(extract_top)

for i in bigrams.take(10):
    print i

bigrams.saveAsTextFile("bigrams")


