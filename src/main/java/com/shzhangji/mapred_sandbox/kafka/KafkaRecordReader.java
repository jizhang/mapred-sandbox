package com.shzhangji.mapred_sandbox.kafka;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class KafkaRecordReader extends RecordReader<Text, Text> {

  long pollTimeout = 30 * 1000; // 30s

  private Consumer<String, String> consumer;
  private TopicPartition topicPartition;
  private long fromOffset;
  private long nextOffset;
  private long untilOffset;
  private Iterator<ConsumerRecord<String, String>> recordIterator;
  private Text key = new Text();
  private Text value = new Text();

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {

    KafkaInputSplit kafkaSplit = (KafkaInputSplit) split;
    topicPartition = new TopicPartition(kafkaSplit.getTopic(), kafkaSplit.getPartition());
    fromOffset = kafkaSplit.getFromOffset();
    nextOffset = fromOffset;

    Properties props = new Properties();
    consumer = new KafkaConsumer<>(props);
    consumer.assign(Arrays.asList(topicPartition));

    consumer.seekToEnd(Arrays.asList(topicPartition));
    untilOffset = consumer.position(topicPartition) + 1;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (nextOffset >= untilOffset) {
      return false;
    }

    if (recordIterator == null) {
      consumer.seek(topicPartition, nextOffset);
      recordIterator = consumer.poll(pollTimeout).iterator();
    }

    if (!recordIterator.hasNext()) {
      throw new IOException(String.format("no data after pollTimeout=%d nextOffset=%d untilOffset=%d",
          pollTimeout, nextOffset, untilOffset));
    }

    ConsumerRecord<String, String> record = recordIterator.next();
    nextOffset = record.offset() + 1;
    key.set(record.key());
    value.set(record.value());

    if (!recordIterator.hasNext()) {
      recordIterator = null;
    }

    return true;
  }

  @Override
  public Text getCurrentKey() throws IOException, InterruptedException {
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
  }

}
