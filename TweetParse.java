// package org.myorg;

import java.io.IOException;
import java.util.*;
import java.util.regex.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
// import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.*;

public class TweetParse {
  public class TweetReader extends RecordReader<LongWritable, Text> {
    private long start; // holds the starting position of the split
    private long pos;
    private long end; // holds the ending position of the split
    private LineReader in;
    private int maxLineLength;
    private LongWritable key = new LongWritable();
    private String tweetValue = new String();
    private String timeValue = new String();
    private Text textLine = new Text();

    /**
     * This method takes as arguments the map task's assigned InputSplit and
     * TaskAttemptContext, and prepares the record reader. For file-based input
     * formats, this is a good place to seek to the byte position in the file to
     * begin reading.
     */
    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {

      // This InputSplit is a FileInputSplit
      FileSplit split = (FileSplit) genericSplit;

      // Retrieve configuration, and Max allowed
      // bytes for a single record
      Configuration job = context.getConfiguration();
      this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);

      // Split "S" is responsible for all records
      // starting from "start" and "end" positions
      start = split.getStart();
      end = start + split.getLength();

      // Retrieve file containing Split "S"
      final Path file = split.getPath();
      FileSystem fs = file.getFileSystem(job);
      FSDataInputStream fileIn = fs.open(split.getPath());

      // If Split "S" starts at byte 0, first line will be processed
      // If Split "S" does not start at byte 0, first line has been already
      // processed by "S-1" and therefore needs to be silently ignored
      boolean skipFirstLine = true; // tweet file has a one line header.

      in = new LineReader(fileIn, job);

      // If first line needs to be skipped, read first line
      // and stores its content to a dummy Text
      if (skipFirstLine) {
        Text dummy = new Text();
        // Reset "start" to "start + line offset"
        start += in.readLine(dummy, 0, (int) Math.min((long) Integer.MAX_VALUE, end - start));
      }

      // Position is the actual start
      this.pos = start;
    }

    /**
     * Like the corresponding method of the InputFormat class, this is an optional
     * method used by the framework for metrics gathering.
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
      if (start == end) {
        return 0.0f;
      } else {
        return Math.min(1.0f, (pos - start) / (float) (end - start));
      }
    }

    public Text getCurrentValue() throws IOException, InterruptedException {
      return new Text(timeValue + tweetValue);
    }

    /**
     * Like the corresponding method of the InputFormat class, this reads a single
     * key/ value pair and returns true until the data is consumed.
     */
    @Override
    public boolean nextKeyValue() throws IOException {
      // Current offset is the key
      key.set(pos);

      int newSize = 0;

      // Make sure we get at least one record that starts in this Split
      while (pos < end) {

        // Read first line and store its content to "value"
        newSize = in.readLine(textLine, maxLineLength,
            Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));

        // No byte read, seems that we reached end of Split
        // Break and return false (no key / value)
        if (newSize == 0) {
          break;
        }

        // Line is read, new position is set
        pos += newSize;

        // Need to discard reads until a 'T' line is encountered.
        timeValue = textLine.toString();
        if (!timeValue.startsWith("T")) {
          continue;
        }

        // discard the 'U' line.
        newSize = in.readLine(textLine, maxLineLength,
            Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
        if (newSize == 0) {
          break;
        }
        pos += newSize;

        // read the 'W' line.
        newSize = in.readLine(textLine, maxLineLength,
            Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
        if (newSize == 0) {
          break;
        }
        pos += newSize;
        tweetValue = textLine.toString();
      }

      if (newSize == 0) {
        // We've reached end of Split
        key = null;
        timeValue = null;
        tweetValue = null;
        return false;
      } else {
        // Tell Hadoop a new line has been found
        // key / value will be retrieved by
        // getCurrentKey getCurrentValue methods
        return true;
      }
    }

    /**
     * This methods are used by the framework to give generated key/value pairs to
     * an implementation of Mapper. Be sure to reuse the objects returned by these
     * methods if at all possible!
     */
    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
      return key;
    }

    public void close() throws IOException {
      if (in != null) {
        in.close();
      }
    }

  }

  // abstract public static class TweetFileInputFormat extends FileInputFormat {
  // private static TweetReader reader;
  // public TweetFileInputFormat(){reader = new TweetReader();};
  // public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
  // TaskAttemptContext context) throws IOException {
  // return reader;
  // }

  // // public RecordReader<LongWritable, Text> getRecordReader(InputSplit s,
  // JobConf c, Reporter r) throws java.io.IOException {
  // // }
  // }

  // public class TweetFileInputFormat extends FileInputFormat {
  // @Override
  // public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
  // TaskAttemptContext context)
  // throws IOException {
  // return new TweetReader();
  // }
  // }

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Pattern pTime = Pattern.compile("(\\d+):(\\d+):(\\d+)");

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);

      if (line.length() > 0) {
        switch (line.charAt(0)) {
          case 'T':
            // We have a time entry, tokenize it.
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
            break;

          case 'W':
            while (tokenizer.hasMoreTokens()) {
              String token = tokenizer.nextToken().toLowerCase();
              if (token.contains("sleep")) {
                //
                // Uncomment to exclude sleep as a substring of other strings.
                //
                // if (token.length() > 5)
                // {
                // Character c = token.charAt(5);
                // if (c >= 'a' && c <= 'z')
                // {
                // break;
                // }
                // }
                word.set("sleep");
                output.collect(word, one);
              }
            }
            break;
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
    JobConf conf = new JobConf(TweetParse.class);
    conf.setJobName("wordcount");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    // conf.setInputFormat(TweetFileInputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    // TweetFileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }
}