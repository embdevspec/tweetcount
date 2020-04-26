cp /mnt/shared/WordCount.java .
hadoop com.sun.tools.javac.Main WordCount.java
jar -cvf WordCount.jar WordCount*.class
hdfs dfs -rm -f -r -skipTrash /user/root/output
hadoop jar WordCount.jar WordCount /user/root/data/tweets.txt /user/root/output
rm -f part-00000
rm ./output.txt
hdfs dfs -get /user/root/output/part-00000 ./output.txt