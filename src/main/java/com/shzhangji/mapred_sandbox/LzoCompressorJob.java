package com.shzhangji.mapred_sandbox;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
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

import com.hadoop.compression.lzo.LzopCodec;

public class LzoCompressorJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Job job = new Job(getConf());
        job.setJarByClass(LzoCompressorJob.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, LzopCodec.class);

        job.setMapperClass(JobMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(JobReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(5);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new LzoCompressorJob(), args));
    }

    public static class JobMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        private LongWritable mapperId = new LongWritable();

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {

            super.setup(context);

            mapperId.set(context.getTaskAttemptID().getTaskID().getId());
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            context.write(mapperId, value);
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

}
