package com.shzhangji.mapred_sandbox.organize;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PartitioningJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        FileSystem.get(getConf()).delete(output, true);

        Job job = new Job(getConf());
        job.setJarByClass(PartitioningJob.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.setMapperClass(JobMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(JobPartitioner.class);

        job.setReducerClass(JobReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(2);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class JobMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] segs = value.toString().split("\\s+");
            long partitionId = 0;
            try {
                partitionId = Long.parseLong(segs[0]);
            } catch (NumberFormatException e) {
                return;
            }
            context.write(new LongWritable(partitionId), value);
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

    public static class JobPartitioner extends Partitioner<LongWritable, Text> implements Configurable {

        private Configuration conf = null;

        @Override
        public Configuration getConf() {
            return conf;
        }

        @Override
        public void setConf(Configuration conf) {
            this.conf = conf;
        }

        @Override
        public int getPartition(LongWritable key, Text value, int numPartitions) {
            int partition = (int) key.get() ;
            if (partition < 0) {
                partition = 0;
            } else if (partition >= numPartitions) {
                partition = numPartitions - 1;
            }
            return partition;
        }

    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new PartitioningJob(), args));
    }
}
