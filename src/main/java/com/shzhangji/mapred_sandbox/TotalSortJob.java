package com.shzhangji.mapred_sandbox;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TotalSortJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new TotalSortJob(), args));
    }

    @Override
    public int run(String[] args) throws Exception {

        Path input = new Path("mapred-sandbox/data/total-sort/");
        Path output = new Path("mapred-sandbox/output/total-sort/");
        Path partition = new Path(output.getParent(), "total-sort-partition");

        FileSystem fs = FileSystem.get(getConf());
        fs.delete(output, true);

        Job job = new Job(getConf());
        job.setJarByClass(TotalSortJob.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.setMapperClass(JobMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(JobReducer.class);
        job.setNumReduceTasks(3);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(LongWritable.class);

        job.setPartitionerClass(TotalOrderPartitioner.class);
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partition);

//        InputSampler.Sampler<LongWritable, NullWritable> sampler =
//                new InputSampler.RandomSampler<LongWritable, NullWritable>(0.9, job.getNumReduceTasks());
//        InputSampler.writePartitionFile(job, sampler);
//
//        for (FileStatus file : fs.listStatus(partition)) {
//
//            SequenceFile.Reader reader = new SequenceFile.Reader(fs, file.getPath(), getConf());
//            LongWritable key = new LongWritable();
//            NullWritable value = NullWritable.get();
//            while (reader.next(key, value)) {
//                System.out.println(key.get());
//            }
//            reader.close();
//
//        }

        SequenceFile.Writer writer = new SequenceFile.Writer(fs, getConf(), partition,
                LongWritable.class, NullWritable.class);
        LongWritable key = new LongWritable();
        NullWritable value = NullWritable.get();
        key.set(4);
        writer.append(key, value);
        key.set(7);
        writer.append(key, value);
        writer.close();

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class JobMapper extends Mapper<LongWritable, Text, LongWritable, NullWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            context.write(new LongWritable(Long.parseLong(value.toString())), NullWritable.get());
        }

    }

    public static class JobReducer extends Reducer<LongWritable, NullWritable, NullWritable, LongWritable> {

        @Override
        protected void reduce(LongWritable key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {

            for (NullWritable value : values) {
                context.write(value, key);
            }
        }

    }

}
