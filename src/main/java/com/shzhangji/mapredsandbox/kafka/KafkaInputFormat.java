package com.shzhangji.mapredsandbox.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

public class KafkaInputFormat extends InputFormat<NullWritable, Text> {
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    List<InputSplit> splits = new ArrayList<>();

    Configuration conf = context.getConfiguration();
    Properties props = new Properties();
    props.put("bootstrap.servers", conf.get(KafkaLoader.CONFIG_BROKERS));
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
      List<PartitionInfo> partitions = consumer.partitionsFor(conf.get(KafkaLoader.CONFIG_TOPIC));

      for (PartitionInfo partition : partitions) {
        long fromOffset = getFromOffset(consumer, partition.topic(), partition.partition(), conf);
        InputSplit split = new KafkaInputSplit(partition.topic(), partition.partition(), fromOffset, partition.leader().host());
        splits.add(split);
      }
    }

    return splits;
  }

  @Override
  public RecordReader<NullWritable, Text> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {

    return new KafkaRecordReader();
  }

  private long getFromOffset(Consumer<String, String> consumer, String topic, int partition,
      Configuration conf) throws IOException {

    Path offsetsPath = new Path(conf.get(KafkaLoader.CONFIG_BASE_PATH), KafkaLoader.OFFSETS_PREFIX);
    Path offsetPath = new Path(offsetsPath, topic + "-" + partition);
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(offsetPath)) {
      try (FSDataInputStream in = fs.open(offsetPath)) {
        return Long.valueOf(in.readUTF()) + 1;
      }
    }
    return 0;
  }
}
