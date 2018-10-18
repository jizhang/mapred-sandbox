package com.shzhangji.mapred_sandbox.kafka;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;

public class KafkaInputSplit extends InputSplit {

  private String topic;
  private int partition;
  private long fromOffset;
  private String[] hosts;

  public KafkaInputSplit(String topic, int partition, long fromOffset, String[] hosts) {
    this.partition = partition;
    this.fromOffset = fromOffset;
    this.hosts = hosts;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return Long.MAX_VALUE;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return hosts;
  }

  public String getTopic() {
    return topic;
  }

  public int getPartition() {
    return partition;
  }

  public long getFromOffset() {
    return fromOffset;
  }

}
