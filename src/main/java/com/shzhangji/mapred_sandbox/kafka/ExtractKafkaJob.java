package com.shzhangji.mapred_sandbox.kafka;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class ExtractKafkaJob extends Configured implements Tool {
  private static final Logger log = LoggerFactory.getLogger(ExtractKafkaJob.class);

  String tableName = "ds_user_action_v3";
  String basePath = "/user/gsbot/kafka";
  String offsetsPrefix = "_offsets";

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();
    conf.setBoolean("mapreduce.map.speculative", false);
    conf.setBoolean("mapreduce.output.fileoutputformat.compress", true);
    conf.set("mapreduce.output.fileoutputformat.compress.codec",
        "com.hadoop.compression.lzo.LzopCodec");

    FileSystem fs = FileSystem.get(conf);

    Path outputPath = new Path("/tmp/jizhang/extract-kafka/job-" + System.currentTimeMillis());

    Job job = Job.getInstance(conf);
    job.setJarByClass(ExtractKafkaJob.class);

    job.setInputFormatClass(KafkaInputFormat.class);
    FileOutputFormat.setOutputPath(job, outputPath);

    job.setMapperClass(ExtractKafkaMapper.class);
    job.setNumReduceTasks(0);

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    boolean isSuccessful = job.waitForCompletion(true);
    if (isSuccessful) {
      // move file
      for (FileStatus partitionDir : fs.listStatus(outputPath)) {
        String partitionDirName = partitionDir.getPath().getName();
        if (!partitionDirName.startsWith("dt=")) {
          continue;
        }

        for (FileStatus file : fs.listStatus(partitionDir.getPath())) {
          if (!file.isFile()) {
            continue;
          }
          Path targetPath = new Path(String.format("%s/%s/%s/%s",
              basePath, tableName, partitionDirName, file.getPath().getName()));
          fs.mkdirs(targetPath.getParent());
          if (fs.exists(targetPath)) {
            fs.delete(targetPath, false);
          }
          fs.rename(file.getPath(), targetPath);
          log.info("move {} to {}", file.getPath(), targetPath);
        }
      }

      // commit offset
      for (FileStatus offsetFile : fs.listStatus(new Path(outputPath, offsetsPrefix))) {
        Path targetPath = new Path(String.format("%s/%s/%s",
            basePath, offsetsPrefix, offsetFile.getPath().getName()));
        fs.mkdirs(targetPath.getParent());
        if (fs.exists(targetPath)) {
          fs.delete(targetPath, false);
        }
        fs.rename(offsetFile.getPath(), targetPath);
        log.info("move {} to {}", offsetFile.getPath(), targetPath);
      }
    }

    fs.delete(outputPath, true);
    log.info("deleted output path {}", outputPath.toString());

    return isSuccessful ? 0 : 2;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new ExtractKafkaJob(), args));
  }
}
