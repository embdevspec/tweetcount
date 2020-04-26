cp /mnt/shared/TweetParse.java .
hadoop com.sun.tools.javac.Main TweetParse.java
result=$?
if [[ $result -ne 0 ]]
then
    return $result
fi

jar -cvf TweetParse.jar TweetParse*.class
hdfs dfs -rm -f -r -skipTrash /user/root/output
hadoop jar TweetParse.jar TweetParse /user/root/data/tweets.txt /user/root/output
rm -f ./output.txt
hdfs dfs -get /user/root/output/part-00000 ./output.txt
cp ./output.txt /mnt/shared
more ./output.txt
