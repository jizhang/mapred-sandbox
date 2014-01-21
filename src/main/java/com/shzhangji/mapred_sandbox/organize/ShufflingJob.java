package com.shzhangji.mapred_sandbox.organize;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ShufflingJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        FileSystem.get(getConf()).delete(output, true);

        Job job = new Job(getConf());
        job.setJarByClass(ShufflingJob.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.setMapperClass(JobMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(JobReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class JobMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        private Random random;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {

            random = new Random();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            context.write(new LongWritable(random.nextLong()), value);
        }

    }

    public static class JobReducer extends Reducer<LongWritable, Text, NullWritable, Text> {

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text value : values) {
                context.write(NullWritable.get(), value);
            }
        }

    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ShufflingJob(), args));
    }
}
