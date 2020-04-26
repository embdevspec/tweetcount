export HADOOP_CLASSPATH=/usr/java/default/lib/tools.jar
export PATH=$PATH:/usr/local/hadoop/bin
hdfs dfs -mkdir /user/root/data
hdfs dfs -put /mnt/shared/tweets.txt /user/root/data
hdfs dfs -ls /user/root/data
set -o vi
