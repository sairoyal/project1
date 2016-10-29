from pyspark import SparkContext
sc = SparkContext()

# creating RDD by parallelize() method
# Python squaring the values in an RDD
num = sc.parallelize([1,2,3,4])
squared = num.map(lambda x: x*x).collect()
#print squared.collect()
for num in squared:
	print "%i" % (num)

#flatMap() in python, splitting lines into words
lines = sc.parallelize(["hello world", "hi"])
words = lines.flatMap(lambda line: line.split(" "))
print words.collect()

#reduce() in python
nums = sc.	
sum = num.reduce(lambda x, y: x+y)
print sum.collect()
