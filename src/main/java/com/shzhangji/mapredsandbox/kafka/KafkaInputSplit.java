package com.shzhangji.mapredsandbox.kafka;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class KafkaInputSplit extends InputSplit implements Writable {

  private String topic;
  private int partition;
  private long fromOffset;
  private String host;

  public KafkaInputSplit() {}

  public KafkaInputSplit(String topic, int partition, long fromOffset, String host) {
    this.topic = topic;
    this.partition = partition;
    this.fromOffset = fromOffset;
    this.host = host;
  }

  @Override
  public long getLength() throws IOException, InterruptedException {
    return Long.MAX_VALUE;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return new String[] { host };
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

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(topic);
    out.writeInt(partition);
    out.writeLong(fromOffset);
    out.writeUTF(host);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    topic = in.readUTF();
    partition = in.readInt();
    fromOffset = in.readLong();
    host = in.readUTF();
  }

}
