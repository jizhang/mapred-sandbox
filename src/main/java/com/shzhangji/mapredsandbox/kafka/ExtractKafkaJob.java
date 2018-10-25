package com.shzhangji.mapredsandbox.kafka;

import java.io.IOException;

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

import com.google.common.base.Preconditions;
import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class ExtractKafkaJob extends Configured implements Tool {
  private static final Logger log = LoggerFactory.getLogger(ExtractKafkaJob.class);

  public static final String CONFIG_TABLE_NAME = "kafka.loader.tableName";
  public static final String CONFIG_BASE_PATH = "kafka.loader.basePath";
  public static final String CONFIG_OUTPUT_PATH = "kafka.loader.outputPath";
  public static final String CONFIG_TOPIC = "kafka.loader.topic";
  public static final String CONFIG_BROKERS = "kafka.loader.brokers";

  public static final String OFFSETS_PREFIX = "_offsets";
  public static final String PARTITION_COLUMN = "dt";
  public static final String TIMESTAMP_KEY = "timestamp";

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();

    // check params
    Preconditions.checkNotNull(conf.get(CONFIG_TABLE_NAME));
    Preconditions.checkNotNull(conf.get(CONFIG_BASE_PATH));
    Preconditions.checkNotNull(conf.get(CONFIG_OUTPUT_PATH));
    Preconditions.checkNotNull(conf.get(CONFIG_TOPIC));
    Preconditions.checkNotNull(conf.get(CONFIG_BROKERS));

    // set configs
    conf.setBoolean("mapreduce.map.speculative", false);
    conf.setBoolean("mapreduce.output.fileoutputformat.compress", true);
    conf.set("mapreduce.output.fileoutputformat.compress.codec",
        "com.hadoop.compression.lzo.LzopCodec");

    // init job
    FileSystem fs = FileSystem.get(conf);
    Path outputPath = new Path(conf.get(CONFIG_OUTPUT_PATH), "job-" + System.currentTimeMillis());
    log.info("output path {}", outputPath);

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
      commitFiles(fs, conf, outputPath);
    }

    fs.delete(outputPath, true);
    log.info("deleted output path {}", outputPath.toString());

    return isSuccessful ? 0 : 2;
  }

  private void commitFiles(FileSystem fs, Configuration conf, Path outputPath) throws IOException {
    String tableName = conf.get(CONFIG_TABLE_NAME);
    String basePath = conf.get(CONFIG_BASE_PATH);
    String partitionPrefix = PARTITION_COLUMN + "=";

    // move file
    for (FileStatus partitionDir : fs.listStatus(outputPath)) {
      String partitionDirName = partitionDir.getPath().getName();
      if (!partitionDirName.startsWith(partitionPrefix)) {
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
        boolean renameOk = fs.rename(file.getPath(), targetPath);
        if (renameOk) {
          log.info("move {} to {}", file.getPath(), targetPath);
        } else {
          throw new IOException(String.format("fail to move %s to %s",
              file.getPath(), targetPath));
        }
      }
    }

    // commit offset
    for (FileStatus offsetFile : fs.listStatus(new Path(outputPath, OFFSETS_PREFIX))) {
      Path targetPath = new Path(String.format("%s/%s/%s",
          basePath, OFFSETS_PREFIX, offsetFile.getPath().getName()));
      fs.mkdirs(targetPath.getParent());
      if (fs.exists(targetPath)) {
        fs.delete(targetPath, false);
      }
      boolean renameOk = fs.rename(offsetFile.getPath(), targetPath);
      if (renameOk) {
        log.info("move {} to {}", offsetFile.getPath(), targetPath);
      } else {
        throw new IOException(String.format("fail to move %s to %s",
            offsetFile.getPath(), targetPath));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new ExtractKafkaJob(), args));
  }
}
