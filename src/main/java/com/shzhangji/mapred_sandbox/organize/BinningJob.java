package com.shzhangji.mapred_sandbox.organize;

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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BinningJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        FileSystem.get(getConf()).delete(output, true);

        Job job = new Job(getConf());
        job.setJarByClass(BinningJob.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.setMapperClass(JobMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setNumReduceTasks(0);

        MultipleOutputs.addNamedOutput(job, "bins", TextOutputFormat.class, NullWritable.class, Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class JobMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

        private MultipleOutputs<NullWritable, Text> mos;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {

            mos = new MultipleOutputs<NullWritable, Text>(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] segs = value.toString().split("\\s+");

            if (segs[0].equals("hadoop")) {
                mos.write("bins", NullWritable.get(), value, "hadoop");
            } else if (segs[0].equals("hive")) {
                mos.write("bins", NullWritable.get(), value, "hive");
            } else {
                mos.write("bins", NullWritable.get(), value, "unknown");
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {

            mos.close();
        }

    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new BinningJob(), args));
    }
}
