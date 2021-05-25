#!/bin/sh

mkdir build

javac -classpath ${HADOOP_CLASSPATH} UmsatzRanking_2.java -d build
jar -cvf team1MapReduce.jar -C build .

$HADOOP_PREFIX/bin/hdfs dfs -rm -r map-reduce-assignment/output/UmsatzRanking_2
$HADOOP_PREFIX/bin/hadoop jar team1MapReduce.jar team1MapReduce.UmsatzRanking_2 map-reduce-assignment/output/GroupByUmsatz_1/part-r-00000 map-reduce-assignment/output/UmsatzRanking_2

$HADOOP_PREFIX/bin/hdfs dfs -cat map-reduce-assignment/output/UmsatzRanking_2/*