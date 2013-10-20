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

public class AutoIncrementId2Job extends Configured implements Tool {

    @Override
    public int run(String[] arg0) throws Exception {

        Path input = new Path("mapred-sandbox/data/auto-increment-id/");
        Path output = new Path("mapred-sandbox/output/auto-increment-id/");

        FileSystem.get(getConf()).delete(output, true);

        Job job = new Job();
        job.setJarByClass(AutoIncrementIdJob.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.setMapperClass(JobMapper.class);
        job.setReducerClass(JobReducer.class);
        job.setNumReduceTasks(1);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new AutoIncrementId2Job(), args));
    }

    public static class JobMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] segs = value.toString().split("\\s+");
            if (segs.length < 1) {
                return;
            }

            long seq;
            try {
                seq = Long.parseLong(segs[0]);
            } catch (NumberFormatException e) {
                return;
            }

            context.write(new LongWritable(seq), value);
        }

    }

    public static class JobReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        private long id;
        private int increment;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {

            super.setup(context);

            id = context.getTaskAttemptID().getTaskID().getId();
            increment = context.getNumReduceTasks();
        }

        protected void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text value : values) {
                id += increment;
                context.write(new LongWritable(id), value);
            }
        }

    }

}
