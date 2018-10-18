package com.shzhangji.mapred_sandbox.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public class KafkaInputFormat extends InputFormat<LongWritable, Text> {

  String topic = "bi.ds_user_action_v3";
  Path basePath = new Path("/user/gsbot/kafka");
  Path offsetsPath = new Path(basePath, "_offsets");
  Path tablePath = new Path(basePath, "ds_user_action_v3");

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    List<InputSplit> splits = new ArrayList<>();

    Properties props = new Properties();
    try(Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
      List<PartitionInfo> partitions = consumer.partitionsFor(topic);

      for (PartitionInfo partition : partitions) {
        long fromOffset = getFromOffset(consumer, partition.partition(), context.getConfiguration());

        List<String> hostList = new ArrayList<>();
        for (Node node : partition.inSyncReplicas()) {
          hostList.add(node.host());
        }
        String[] hosts = hostList.toArray(new String[0]);

        InputSplit split = new KafkaInputSplit(topic, partition.partition(), fromOffset, hosts);
        splits.add(split);
      }
    }

    return splits;
  }

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {



    return null;
  }

  long getFromOffset(Consumer<String, String> consumer, int partition, Configuration conf) throws IOException {
    Path offsetPath = new Path(offsetsPath, topic + "-" + partition);
    try(FileSystem fs = FileSystem.get(conf)) {
      if (fs.exists(offsetPath)) {
        try (FSDataInputStream in = fs.open(offsetPath)) {
          return in.readLong() + 1;
        }
      }
      return 0L;
    }
  }

  OffsetRange getOffsetRange(Consumer<String, String> consumer, int partition, Configuration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);

    TopicPartition topicPartition = new TopicPartition(topic, partition);

    Path offsetPath = new Path(offsetsPath, topic + "-" + partition);

    long fromOffset = 0;
    if (fs.exists(offsetPath)) {
      try (FSDataInputStream in = fs.open(offsetPath)) {
        fromOffset = in.readLong() + 1;
      }
    } else {
      consumer.seekToBeginning(Arrays.asList(topicPartition));
      fromOffset = consumer.position(topicPartition);
    }

    consumer.seekToEnd(Arrays.asList(topicPartition));
    long untilOffset = consumer.position(topicPartition) + 1;

    return new OffsetRange(fromOffset, untilOffset);
  }

}
