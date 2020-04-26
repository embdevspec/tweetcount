// package org.myorg;

import java.io.IOException;
import java.util.*;
import java.util.regex.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class WordCount {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Pattern pTime = Pattern.compile("(\\d+):(\\d+):(\\d+)");

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      String line = value.toString();

      if (line.length() > 0 && line.charAt(0) == 'T') {
        // We have a time entry, tokenize it.
        StringTokenizer tokenizer = new StringTokenizer(line);
        if (tokenizer.hasMoreTokens()) {
          tokenizer.nextToken(); // skip over 'T'
          tokenizer.nextToken(); // skip over date

          // match for time.
          String recordTime = tokenizer.nextToken();
          Matcher m = pTime.matcher(recordTime);
          if (m.matches()) {
            word.set("" + m.group(1));
            output.collect(word, one);
          }
        }
      }
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
        Reporter reporter) throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      output.collect(key, new IntWritable(sum));
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(WordCount.class);
    conf.setJobName("wordcount");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }
}