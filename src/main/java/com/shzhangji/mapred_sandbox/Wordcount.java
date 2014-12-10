package com.shzhangji.mapred_sandbox;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Wordcount extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Path input = new Path(args[0]);
        Path output = new Path("/tmp/jizhang/wordcount");
        FileSystem.get(getConf()).delete(output, true);
        Job job = new Job(getConf());
        job.setJarByClass(Wordcount.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            for (String word : value.toString().split(" ")) {
                context.write(new Text(word), new LongWritable(1));
            }
        }
    }
    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Wordcount(), args));
    }
}
