package com.shzhangji.mapred_sandbox;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LineCounterJob extends Configured implements Tool {

    public static final String COUNTER_GROUP = "MapRed Sandbox Counters";
    public static final String COUNTER_NAME = "Line count";

    @Override
    public int run(String[] args) throws Exception {

        Path input = new Path(args[0]);
        Path output = new Path("/tmp/jizhang/line-count");

        FileSystem fs = FileSystem.get(getConf());
        fs.delete(output, true);

        Job job = new Job(getConf());
        job.setJarByClass(LineCounterJob.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.setMapperClass(JobMapper.class);
        job.setNumReduceTasks(0);

        if (!job.waitForCompletion(true)) {
            return 0;
        }

        long lineCount = job.getCounters().findCounter(COUNTER_GROUP, COUNTER_NAME).getValue();
        fs.delete(output, true);
        fs.create(output, true).writeUTF(String.valueOf(lineCount));

        return 1;
    }

    public static class JobMapper extends Mapper<LongWritable, Text, NullWritable, LongWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            context.getCounter(COUNTER_GROUP, COUNTER_NAME).increment(1);
        }

    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new LineCounterJob(), args));
    }

}
