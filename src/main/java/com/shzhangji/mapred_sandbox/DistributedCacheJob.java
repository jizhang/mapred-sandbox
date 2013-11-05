package com.shzhangji.mapred_sandbox;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
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

public class DistributedCacheJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Path input = new Path("mapred-sandbox/data/auto-increment-id/");
        Path output = new Path("mapred-sandbox/output/auto-increment-id/");

        FileSystem.get(getConf()).delete(output, true);

        Job job = new Job(getConf());
        job.setJarByClass(DistributedCacheJob.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.setMapperClass(JobMapper.class);
        job.setNumReduceTasks(0);;

        DistributedCache.addArchiveToClassPath(
                new Path("/tmp/mysql-connector-java-5.1.26.jar"),
                job.getConfiguration(), FileSystem.getLocal(getConf()));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new DistributedCacheJob(), args));
    }

    public static class JobMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {

            try {
                System.out.println(Class.forName("com.mysql.jdbc.Connection").toString());
            } catch (ClassNotFoundException e) {
                System.out.println(e.toString());
            }
        }

    }

}
