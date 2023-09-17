# Databricks notebook source
print('Puua')

# COMMAND ----------

8.8 KB
stupid_sample.xlsx
Remove file
File uploaded to /FileStore/tables/stupid_sample-2.xlsx

# COMMAND ----------

from pyspark import SparkConf, SparkContext

# COMMAND ----------

import com.crealytics.spark.excel._

# COMMAND ----------

conf=SparkConf().setAppName('read_file')

# COMMAND ----------

sc=SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

text=sc.textFile('/FileStore/tables/new_stupid_file.txt')

# COMMAND ----------

text

# COMMAND ----------

text2=sc.addFile('/FileStore/tables/stupid_sample-2.xlsx')

# COMMAND ----------

text.collect()

# COMMAND ----------

text1= text.map(lambda x:x+' is a number')

# COMMAND ----------

text1.collect()

# COMMAND ----------

a=text.map(lambda x:int(x[0])-1)

# COMMAND ----------

a.collect()

# COMMAND ----------

# DBTITLE 1,first function foo
def foo(x):
    l2=[]
    l=x.split()
    for s in l:
        l2.append(s+' puja here')
    return l2

text_int=text.map(foo)

# COMMAND ----------

text_int.collect()

# COMMAND ----------



# COMMAND ----------

quiz=sc.textFile('/FileStore/tables/stupid_rdd_quiz')

# COMMAND ----------

quiz.collect()

# COMMAND ----------

quiz.map(lambda x: [len(a) for a in x.split(' ')]).collect()

# COMMAND ----------

def getlen(x):
    arr=[]
    for word in x.split(' '):
        arr.append(len(word))
    return arr

res=quiz.map(getlen)


# COMMAND ----------

res.collect()

# COMMAND ----------

# DBTITLE 1,FlatMap Practice


# COMMAND ----------

from pyspark import SparkConf, SparkContext
conf=SparkConf().setAppName('FlatMap')

# COMMAND ----------

sc=SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

rdd=sc.textFile('/FileStore/tables/new_stupid_file.txt')

# COMMAND ----------

rdd.collect()

# COMMAND ----------

fmap_out= rdd.flatMap(lambda x:x.split(' '))

# COMMAND ----------

fmap_out.collect()

# COMMAND ----------

def geteven(x):
    if x=='1 2 3 ': #whenever com=ndition met that input is returned as output
        return True
    else: ##whenever com=ndition met that input is not returned
        return False 
fdata=rdd.filter(geteven)
fdata.collect()


# COMMAND ----------

# DBTITLE 1,Filter Quiz
quiz= sc.textFile('/FileStore/tables/spark_stupid_filter_quiz')
quiz.collect()

# COMMAND ----------

def remove(x):
    if x.startswith('a') or x.startswith('c'):
        return False
    else:
        return True 

quizf=quiz.flatMap(lambda x:x.split(' '))
#quizf.collect()
quizf.filter(remove).collect()



# COMMAND ----------

quizf.filter(lambda x:not (x.startswith('a') or x.startswith('c')) ).collect() #great if else 

# COMMAND ----------

# DBTITLE 1,Distinct 
rdd=sc.textFile('/FileStore/tables/new_stupid_file.txt')

# COMMAND ----------

rdd.distinct().collect()

# COMMAND ----------

rdd.flatMap(lambda x:x.split(' ')).distinct().collect()


# COMMAND ----------

# DBTITLE 1,groupByKey() and mapValue()
#data is in key value format
quiz= sc.textFile('/FileStore/tables/spark_stupid_filter_quiz')
quiz.collect()

# COMMAND ----------

rdd_grp=quiz.flatMap(lambda x:x.split(' ')).map(lambda x:(len(x),x))
rdd_grp.collect()

# COMMAND ----------

rdd_grp.groupByKey().mapValues(list).collect()

# COMMAND ----------

# DBTITLE 1,reduceByKey()- needs the functionality byt which need to reduce the data
# so x, y represent the first two elements in rdd and the y would be 3rd elements and x ould be result of first computation/ reduction done
#for key 6- x,y
#           animal,laptop=> animal laptop
#           animal laptop,switch =>animal laptop switch 
#its like recursion a bit
rdd_grp.reduceByKey(lambda x,y:(x+' '+y)).collect()

# COMMAND ----------

# DBTITLE 1,Quiz on groupby and reduce by key
quiz= sc.textFile('/FileStore/tables/spark_stupid_filter_quiz-1')
quiz.collect()

# COMMAND ----------

quiz.flatMap(lambda x: x.split(' ')).filter(lambda x: x!='').map(lambda x:(x,len(x))).countByKey()

# COMMAND ----------

#dont like this approach so much 😒 when i can use countByKey
a=quiz.flatMap(lambda x: x.split(' ')).filter(lambda x: x!='').map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
a.collect()


# COMMAND ----------

# DBTITLE 1,rdd.count() and rdd.countByValue()- action, no need for collect 
#countByValue is exactly same as collections.Counter iin python
quiz.flatMap(lambda x: x.split(' ')).filter(lambda x: x!='').countByValue()

# COMMAND ----------

# DBTITLE 1,rdd.saveAsTextFile()
export=quiz.flatMap(lambda x: x.split(' ')).filter(lambda x: x!='').countByValue()
#sc.parallelize(export).saveAsTextFile('/FileStore/tables/text_count_outfile')

sc.parallelize(export).collect()

# COMMAND ----------

new_file=sc.textFile('/FileStore/tables/text_count_outfile')
new_file.getNumPartitions()

# COMMAND ----------

new_file.getNumPartitions()

# COMMAND ----------

new_file.collect()

# COMMAND ----------

# DBTITLE 1,rdd.repartition()-to increase partitions and coalesce()-to reduce partitions
new_file=new_file.repartition(10)

# COMMAND ----------

new_file.getNumPartitions()

# COMMAND ----------

new_file=new_file.coalesce(5)

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Movie rating Average

movie_ratings=sc.textFile('/FileStore/tables/movie_ratings.csv')

# COMMAND ----------

movie_ratings.collect()

# COMMAND ----------

trasf_movie_ratings= movie_ratings.map(lambda x:(x.split(',')[0],(int(x.split(',')[1]),1)))

# COMMAND ----------

trasf_movie_ratings=trasf_movie_ratings.reduceByKey(lambda x,y:(x[0]+y[0]/(x[1]+y[1]),x[1]+y[1]))
trasf_movie_ratings.collect()


# COMMAND ----------

a=movie_ratings.map(lambda x:(x.split(',')[0],(int(x.split(',')[1]),1))).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
a.map(lambda a:(a[0],a[1][0]/a[1][1])).collect()
#.reduceByKey(lambda x,y:x[0],x[1][0]/[1][1]).collect()

# COMMAND ----------

# DBTITLE 1,Average Quiz
avg_quiz= sc.textFile('/FileStore/tables/average_quiz_sample.csv')
avg_quiz.collect()

# COMMAND ----------

trasf_quiz=avg_quiz.map(lambda x:(x.split(',')[0],(float(x.split(',')[2]),1)))
trasf_quiz.collect()

# COMMAND ----------

# in this when you do
# lambda  x,   y     :  x[0]+y[0],  x[1]+y[1]
#     (3.0,1)(1.0,1)      3.0+ 1.0  ,   1+1     => (4,2)
#     (4.0,2)(2.0,1)      4.0+ 2.0  ,   2+1     => (6,3)
#this is how reduceByKey works

trasf_quiz=trasf_quiz.reduceByKey(lambda x ,y :(x[0]+y[0],x[1]+y[1]))
trasf_quiz.collect()

# COMMAND ----------

trasf_quiz.map(lambda a: (a[0],a[1][0]/a[1][1])).collect()

# COMMAND ----------

# DBTITLE 1,MIn and Max
movie_ratings.collect()

# COMMAND ----------

movie_ratings.map(lambda x: (x.split(',')[0],int(x.split(',')[1]))).reduceByKey(lambda x,y: x if x<y else y).collect()


# COMMAND ----------

movie_ratings.map(lambda x: (x.split(',')[0],int(x.split(',')[1]))).reduceByKey(lambda x,y: y if x<y else x).collect()

# COMMAND ----------

# DBTITLE 1,min max quiz
avg_quiz.collect()

# COMMAND ----------

# DBTITLE 1,min
avg_quiz.map(lambda x: (x.split(',')[1],float(x.split(',')[2]))).reduceByKey(lambda x,y: x if x<y else y).collect()

# COMMAND ----------

# DBTITLE 1,max
avg_quiz.map(lambda x: (x.split(',')[1],float(x.split(',')[2]))).reduceByKey(lambda x,y: y if x<y else x).collect()

# COMMAND ----------

# DBTITLE 1,Mini Project- on so far learnt
dataset=sc.addFile('/FileStore/tables/StudentData-1.csv')

# COMMAND ----------

dataset=sc.textFile('/FileStore/tables/Mini_project_quiz.csv')
dataset.collect()

# COMMAND ----------

dataset.collect()

# COMMAND ----------

dataset.getNumPartitions()

# COMMAND ----------

header=dataset.first()
print(header)   #'age,gender,name,course,roll,marks,email'
# or better way is header=rdd.first()
dataset=dataset.filter(lambda x: x!=header)
dataset.collect()

# COMMAND ----------

daatset= dataset.map(lambda x: x.split(','))
daatset.collect()

# COMMAND ----------

# DBTITLE 1,get number of student in file
daatset.map(lambda x:x[2]).distinct().count()


# COMMAND ----------

daatset.count()

# COMMAND ----------

# DBTITLE 1,total marks by male and female
daatset.map(lambda x: (x[1],int(x[5]))).reduceByKey(lambda x,y:x+y).collect()

# COMMAND ----------

# DBTITLE 1,total number of student who passed and failed
passed=daatset.filter(lambda x:int(x[5])>50).count()
failed=daatset.filter(lambda x:int(x[5])<=50).count()
print('passed ',passed, ' failed ',failed)

# COMMAND ----------

# DBTITLE 1,total number of student per course
#daatset.collect()
daatset.map(lambda x:x[3]).countByValue()

# COMMAND ----------

# DBTITLE 1,total marks for each course
daatset.map(lambda x:(x[3],int(x[5]))).reduceByKey(lambda x,y:x+y).collect()

# COMMAND ----------

# DBTITLE 1,average marks per course
a=daatset.map(lambda x:(x[3],(int(x[5]),1))).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
a.map(lambda x: (x[0],x[1][0]/x[1][1])).collect()

# COMMAND ----------

# DBTITLE 1,Min marks per course
daatset.map(lambda x:(x[3],int(x[5]))).reduceByKey(lambda x,y:x if x<y else y).collect()

# COMMAND ----------

# DBTITLE 1,max marks per course
daatset.map(lambda x:(x[3],int(x[5]))).reduceByKey(lambda x,y:y if x<y else x).collect()

# COMMAND ----------

# DBTITLE 1,average age of male and female student
a=daatset.map(lambda x:(x[1],(int(x[0]),1))).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
#a.collect()
a.map(lambda x: (x[0],x[1][0]/x[1][1])).collect()

# COMMAND ----------


