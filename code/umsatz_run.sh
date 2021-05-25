#!/bin/sh

mkdir build

javac -classpath ${HADOOP_CLASSPATH} GroupByUmsatz_1.java -d build
jar -cvf team1MapReduce.jar -C build .

$HADOOP_PREFIX/bin/hdfs dfs -rm -r map-reduce-assignment/output/GroupByUmsatz_1
$HADOOP_PREFIX/bin/hadoop jar team1MapReduce.jar team1MapReduce.GroupByUmsatz_1 map-reduce-assignment/input/purchases.txt map-reduce-assignment/output/GroupByUmsatz_1

$HADOOP_PREFIX/bin/hdfs dfs -cat map-reduce-assignment/output/GroupByUmsatz_1/*