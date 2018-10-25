package com.shzhangji.mapredsandbox.cartesian;

import java.io.IOException;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.join.CompositeInputSplit;
import org.apache.hadoop.util.ReflectionUtils;

@SuppressWarnings("rawtypes")
public class CartesianInputFormat extends FileInputFormat {

    public static final String LEFT_INPUT_FORMAT = "cart.left.inputformat";
    public static final String LEFT_INPUT_PATH = "cart.left.path";
    public static final String RIGHT_INPUT_FORMAT = "cart.right.inputformat";
    public static final String RIGHT_INPUT_PATH = "cart.right.path";

    public static void setLeftInputInfo(JobConf job,
            Class<? extends FileInputFormat> inputFormat,
            String inputPath) {

        job.set(LEFT_INPUT_FORMAT, inputFormat.getCanonicalName());
        job.set(LEFT_INPUT_PATH, inputPath);
    }

    public static void setRightInputInfo(JobConf job,
            Class<? extends FileInputFormat> inputFormat,
            String inputPath) {

        job.set(RIGHT_INPUT_FORMAT, inputFormat.getCanonicalName());
        job.set(RIGHT_INPUT_PATH, inputPath);
    }

    @SuppressWarnings("static-access")
    private InputSplit[] getInputSplits(JobConf conf, String inputFormatClass,
            String inputPath, int numSplits) throws ClassNotFoundException, IOException {

        FileInputFormat inputFormat = (FileInputFormat) ReflectionUtils
                .newInstance(Class.forName(inputFormatClass), conf);
        inputFormat.setInputPaths(conf, inputPath);
        return inputFormat.getSplits(conf, numSplits);
    }

    @Override
    public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {

        InputSplit[] leftSplits, rightSplits;
        try {
            leftSplits = getInputSplits(conf, conf.get(LEFT_INPUT_FORMAT),
                    conf.get(LEFT_INPUT_PATH), numSplits);
            rightSplits = getInputSplits(conf, conf.get(RIGHT_INPUT_FORMAT),
                    conf.get(RIGHT_INPUT_PATH), numSplits);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        CompositeInputSplit[] returnSplits = new CompositeInputSplit[leftSplits.length * rightSplits.length];

        int i = 0;
        for (InputSplit left : leftSplits) {
            for (InputSplit right : rightSplits) {
                returnSplits[i] = new CompositeInputSplit(2);
                returnSplits[i].add(left);
                returnSplits[i].add(right);
                ++i;
            }
        }

        LOG.info("Total splits to process: " + returnSplits.length);
        return returnSplits;
    }

    @Override
    public RecordReader getRecordReader(InputSplit split, JobConf conf,
            Reporter reporter) throws IOException {

        return new CartesianRecordReader((CompositeInputSplit) split, conf, reporter);
    }

}
