package com.shzhangji.mapred_sandbox.kafka;

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

public class ExtractKafkaMapper extends Mapper<NullWritable, Text, NullWritable, Text>  {
  private MultipleOutputs<NullWritable, Text> mos;
  private ObjectMapper objectMapper = new ObjectMapper();

  @Override
  protected void setup(Mapper<NullWritable, Text, NullWritable, Text>.Context context)
      throws IOException, InterruptedException {

    mos = new MultipleOutputs<NullWritable, Text>(context);
  }

  @Override
  protected void map(NullWritable key, Text value,
      Mapper<NullWritable, Text, NullWritable, Text>.Context context)
      throws IOException, InterruptedException {

    JsonNode node = objectMapper.readTree(value.toString());
    long timestamp = node.path("timestamp").getLongValue();
    Date date = new Date(timestamp * 1000);
    DateFormat df = new SimpleDateFormat("yyyyMMdd");
    String partition = df.format(date);
    mos.write(key, value, "dt=" + partition + "/part");
  }

  @Override
  protected void cleanup(Mapper<NullWritable, Text, NullWritable, Text>.Context context)
      throws IOException, InterruptedException {

    mos.close();
  }
}
