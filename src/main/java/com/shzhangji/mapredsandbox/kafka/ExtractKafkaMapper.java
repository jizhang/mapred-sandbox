package com.shzhangji.mapredsandbox.kafka;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtractKafkaMapper extends Mapper<NullWritable, Text, NullWritable, Text>  {
  private static final Logger log = LoggerFactory.getLogger(ExtractKafkaMapper.class);

  private MultipleOutputs<NullWritable, Text> mos;
  private ObjectMapper objectMapper = new ObjectMapper();
  private KafkaInputSplit split;

  @Override
  protected void setup(Mapper<NullWritable, Text, NullWritable, Text>.Context context)
      throws IOException, InterruptedException {

    mos = new MultipleOutputs<NullWritable, Text>(context);
    split = (KafkaInputSplit) context.getInputSplit();
  }

  @Override
  protected void map(NullWritable key, Text value,
      Mapper<NullWritable, Text, NullWritable, Text>.Context context)
      throws IOException, InterruptedException {

    String partitionValue;
    try {
      partitionValue = getPartitionValue(value.toString());
    } catch (Exception e) {
      log.warn("Fail to get partition value - {}", e.getMessage());
      return;
    }

    String baseOutputPath = String.format("dt=%s/%s-%d-%d", partitionValue,
        split.getTopic(), split.getPartition(), split.getFromOffset());
    mos.write(key, value, baseOutputPath);
  }

  @Override
  protected void cleanup(Mapper<NullWritable, Text, NullWritable, Text>.Context context)
      throws IOException, InterruptedException {

    mos.close();
  }

  private String getPartitionValue(String value) throws IOException {
    JsonNode node = objectMapper.readTree(value);
    long timestamp = node.path("timestamp").getLongValue();
    Date date = new Date(timestamp * 1000);
    DateFormat df = new SimpleDateFormat("yyyyMMdd");
    String partitionValue = df.format(date);
    return partitionValue;
  }
}
