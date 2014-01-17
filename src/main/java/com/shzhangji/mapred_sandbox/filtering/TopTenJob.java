package com.shzhangji.mapred_sandbox.filtering;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

public class TopTenJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        Job job = new Job(getConf());
        job.setJarByClass(TopTenJob.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.setMapperClass(JobMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(JobReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(LongWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class JobMapper extends Mapper<LongWritable, Text, NullWritable, LongWritable> {

        private List<Long> numbers;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {

            numbers = new ArrayList<Long>();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            try {
                numbers.add(Long.valueOf(value.toString()));
            } catch (NumberFormatException e) {
                return;
            }

            Collections.sort(numbers, Collections.reverseOrder());
            if (numbers.size() > 10) {
                numbers.remove(10);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {

            for (Long number : numbers) {
                context.write(NullWritable.get(), new LongWritable(number));
            }
        }

    }

    public static class JobReducer extends Reducer<NullWritable, LongWritable, NullWritable, LongWritable> {

        @Override
        protected void reduce(NullWritable key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            List<Long> numbers = new ArrayList<Long>();
            for (LongWritable value : values) {
                numbers.add(value.get());
            }

            Collections.sort(numbers, Collections.reverseOrder());
            for (Long number : numbers.subList(0, 10)) {
                context.write(NullWritable.get(), new LongWritable(number));
            }
        }

    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new TopTenJob(), args));
    }
}
