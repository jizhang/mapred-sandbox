package com.shzhangji.mapred_sandbox;

import java.io.IOException;
import java.net.URI;

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

        DistributedCache.addCacheFile(new URI("file:/home/jizhang/git/mapred-sandbox/README.md"), job.getConfiguration());

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new DistributedCacheJob(), args));
    }

    public static class JobMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {

            Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            for (Path file : files) {
                System.out.println(file.toString());
            }

        }

    }

}
