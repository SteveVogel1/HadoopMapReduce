$HADOOP_PREFIX/bin/hdfs dfs -mkdir -p map-reduce-assignment/input
cd $HADOOP_PREFIX
export HADOOP_CLASSPATH=$(bin/hadoop classpath)

$HADOOP_PREFIX/bin/hdfs dfs -copyFromLocal /home/ddm/src/code/purchases.txt map-reduce-assignment/input/purchases.txt