package com.shzhangji.mapred_sandbox;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MultipleOutputsJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Path input = new Path("mapred-sandbox/data/multiple-outputs/");
        Path output = new Path("mapred-sandbox/output/multiple-outputs/");

        FileSystem.get(getConf()).delete(output, true);

        Job job = new Job(getConf());
        job.setJarByClass(MultipleOutputsJob.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.setReducerClass(JobReducer.class);
        job.setNumReduceTasks(1);

        MultipleOutputs.addNamedOutput(job, "seq", SequenceFileOutputFormat.class, NullWritable.class, IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MultipleOutputsJob(), args));
    }

    public static class JobReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        private MultipleOutputs<LongWritable, Text> mos;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {

            super.setup(context);
            mos = new MultipleOutputs<LongWritable, Text>(context);
        }

        protected void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text value : values) {
                context.write(key, value);

                if (key.get() % 2 == 0) {
                    mos.write(key, value, "even/part");
                } else {
                    mos.write("seq", NullWritable.get(), new IntWritable(value.toString().length()), "odd/seq");
                }
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            super.cleanup(context);
            mos.close();
        }

    }

}
