package com.shzhangji.mapredsandbox;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AutoIncrementIdJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Path input = new Path("mapred-sandbox/data/auto-increment-id/");
        Path output = new Path("mapred-sandbox/output/auto-increment-id/");

        FileSystem.get(getConf()).delete(output, true);

        Job job = new Job();
        job.setJarByClass(AutoIncrementIdJob.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.setMapperClass(JobMapper.class);
        job.setNumReduceTasks(0);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new AutoIncrementIdJob(), args));
    }

    public static class JobMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        private long id;
        private int increment;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {

            super.setup(context);

            id = context.getTaskAttemptID().getTaskID().getId();
            increment = context.getConfiguration().getInt("mapred.map.tasks", 0);
            if (increment == 0) {
                throw new IllegalArgumentException("mapred.map.tasks is zero");
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            id += increment;
            context.write(new LongWritable(id),
                    new Text(String.format("%d, %s", key.get(), value.toString())));
        }

    }

}
