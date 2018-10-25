package com.shzhangji.mapredsandbox.kafka;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaRecordReader extends RecordReader<NullWritable, Text> {

  private static final Logger log = LoggerFactory.getLogger(KafkaRecordReader.class);

  private long pollTimeout = 10_000;

  private JobContext job;
  private Consumer<String, String> consumer;
  private TopicPartition topicPartition;
  private long fromOffset;
  private long nextOffset;
  private long untilOffset;
  private Iterator<ConsumerRecord<String, String>> recordIterator;
  private NullWritable key = NullWritable.get();
  private Text value = new Text();

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {

    job = context;

    KafkaInputSplit kafkaSplit = (KafkaInputSplit) split;
    topicPartition = new TopicPartition(kafkaSplit.getTopic(), kafkaSplit.getPartition());
    fromOffset = kafkaSplit.getFromOffset();
    nextOffset = fromOffset;

    Configuration conf = job.getConfiguration();
    Properties props = new Properties();
    props.put("bootstrap.servers", conf.get(ExtractKafkaJob.CONFIG_BROKERS));
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("enable.auto.commit", "false");

    consumer = new KafkaConsumer<>(props);
    consumer.assign(Arrays.asList(topicPartition));

    consumer.seekToEnd(Arrays.asList(topicPartition));
    untilOffset = consumer.position(topicPartition) + 1;

    log.info("fromOffset={} untilOffset={}", fromOffset, untilOffset);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (nextOffset >= untilOffset) {
      return false;
    }

    if (recordIterator == null || !recordIterator.hasNext()) {
      consumer.seek(topicPartition, nextOffset);
      recordIterator = consumer.poll(pollTimeout).iterator();
    }

    if (!recordIterator.hasNext()) {
      throw new IOException(String.format("no data after pollTimeout=%d nextOffset=%d untilOffset=%d",
          pollTimeout, nextOffset, untilOffset));
    }

    ConsumerRecord<String, String> record = recordIterator.next();
    if (record.offset() >= untilOffset) {
      return false;
    }

    nextOffset = record.offset() + 1;
    value.set(record.value());
    return true;
  }

  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return (float) (nextOffset - fromOffset) / (untilOffset - fromOffset);
  }

  @Override
  public void close() throws IOException {
    consumer.close();

    Path outputPath = FileOutputFormat.getOutputPath(job);
    Path offsetPath = new Path(outputPath, String.format("%s/%s-%s",
        ExtractKafkaJob.OFFSETS_PREFIX, topicPartition.topic(), topicPartition.partition()));
    FileSystem fs = FileSystem.get(job.getConfiguration());
    try (FSDataOutputStream out = fs.create(offsetPath, true)) {
      out.writeUTF(String.valueOf(untilOffset));
    }
  }
}
