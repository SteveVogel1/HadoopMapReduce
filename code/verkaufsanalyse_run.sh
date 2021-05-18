#!/bin/sh

javac -classpath ${HADOOP_CLASSPATH} src/Verkaufsanalyse.java -d build
jar -cvf team1MapReduce.jar -C build .

$HADOOP_PREFIX/bin/hdfs dfs -rm -r map-reduce-assignment/output/Verkaufsanalyse.txt
$HADOOP_PREFIX/bin/hadoop jar team1MapReduce.jar team1MapReduce.Verkaufsanalyse map-reduce-assignment/input/purchases.txt map-reduce-assignment/output/Verkaufsanalyse.txt

$HADOOP_PREFIX/bin/hdfs dfs -cat map-reduce-assignment/output/Verkaufsanalyse.txt