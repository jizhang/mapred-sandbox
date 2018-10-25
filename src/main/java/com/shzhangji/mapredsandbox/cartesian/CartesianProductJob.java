package com.shzhangji.mapredsandbox.cartesian;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class CartesianProductJob {

    public static void main(String[] args) throws Exception {

        JobConf conf = new JobConf("Cartesian Product");
        conf.setJarByClass(CartesianProductJob.class);
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");

        conf.setMapperClass(CartesianMapper.class);
        conf.setNumReduceTasks(0);

        conf.setInputFormat(CartesianInputFormat.class);

        String inputPath = "README.md";
        CartesianInputFormat.setLeftInputInfo(conf, TextInputFormat.class, inputPath);
        CartesianInputFormat.setRightInputInfo(conf, TextInputFormat.class, inputPath);

        Path outputPath = new Path("build/out");
        FileSystem.get(conf).delete(outputPath, true);
        TextOutputFormat.setOutputPath(conf, outputPath);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        RunningJob job = JobClient.runJob(conf);
        while (!job.isComplete()) {
            Thread.sleep(1000);
        }

        System.exit(job.isSuccessful() ? 0 : 1);

    }

}
