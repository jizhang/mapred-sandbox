package com.shzhangji.mapred_sandbox.kafka;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class KafkaInputSplit extends InputSplit implements Writable {

  private String topic;
  private int partition;
  private long fromOffset;
  private String[] hosts;

  public KafkaInputSplit() {}

  public KafkaInputSplit(String topic, int partition, long fromOffset, String[] hosts) {
    this.topic = topic;
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

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(topic);
    out.writeInt(partition);
    out.writeLong(fromOffset);

    out.writeInt(hosts.length);
    for (String host : hosts) {
      out.writeUTF(host);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    topic = in.readUTF();
    partition = in.readInt();
    fromOffset = in.readLong();

    int numHosts = in.readInt();
    List<String> hostList = new ArrayList<>();
    for (int i = 0; i < numHosts; ++i) {
      hostList.add(in.readUTF());
    }
    hosts = hostList.toArray(new String[0]);
  }

}
