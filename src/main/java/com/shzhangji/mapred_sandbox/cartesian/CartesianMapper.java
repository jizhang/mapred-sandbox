package com.shzhangji.mapred_sandbox.cartesian;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CartesianMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

    private Text outkey = new Text();

    public void map(Text key, Text value, OutputCollector<Text, Text> output,
            Reporter reporter) throws IOException {

        if (!key.toString().equals(value.toString())) {

            String[] leftTokens = key.toString().split("\\s");
            String[] rightTokens = value.toString().split("\\s");

            HashSet<String> leftSet = new HashSet<String>(Arrays.asList(leftTokens));
            HashSet<String> rightSet = new HashSet<String>(Arrays.asList(rightTokens));

            int sameWordCount = 0;
            StringBuilder words = new StringBuilder();
            for (String s: leftSet) {
                if (rightSet.contains(s)) {
                    words.append(s + ",");
                    ++sameWordCount;
                }
            }

            if (sameWordCount > 2) {
                outkey.set(words + "\t" + key);
                output.collect(outkey, value);
            }
        }

    }

}
