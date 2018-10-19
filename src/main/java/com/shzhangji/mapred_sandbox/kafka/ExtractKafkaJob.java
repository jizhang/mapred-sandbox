package com.shzhangji.mapred_sandbox.kafka;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class ExtractKafkaJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.setBoolean("mapreduce.map.speculative", false);
        conf.setBoolean("mapreduce.output.fileoutputformat.compress", true);
        conf.set("mapreduce.output.fileoutputformat.compress.codec", "com.hadoop.compression.lzo.LzopCodec");

        FileSystem fs = FileSystem.get(conf);

        Path output = new Path("/tmp/jizhang/mapred-sandbox/output/extract-kafka");
        fs.delete(output, true);

        Job job = Job.getInstance(conf);
        job.setJarByClass(ExtractKafkaJob.class);

        job.setInputFormatClass(KafkaInputFormat.class);
        FileOutputFormat.setOutputPath(job, output);

        job.setMapperClass(ExtractKafkaMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ExtractKafkaJob(), args));
    }
}
