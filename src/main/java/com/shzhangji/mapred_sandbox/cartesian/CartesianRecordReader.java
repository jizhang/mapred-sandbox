package com.shzhangji.mapred_sandbox.cartesian;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.join.CompositeInputSplit;
import org.apache.hadoop.util.ReflectionUtils;

@SuppressWarnings(value = {"rawtypes", "unchecked"})
public class CartesianRecordReader<K1, V1, K2, V2> implements RecordReader<Text, Text> {

    private RecordReader leftRR = null, rightRR = null;

    private FileInputFormat rightFIF;
    private JobConf rightConf;
    private InputSplit rightIS;
    private Reporter rightReporter;

    private K1 lkey;
    private V1 lvalue;
    private K2 rkey;
    private V2 rvalue;
    private boolean goToNextLeft = true, alldone = false;

    public CartesianRecordReader(CompositeInputSplit split, JobConf conf,
            Reporter reporter) throws IOException {

        this.rightConf = conf;
        this.rightIS = split.get(1);
        this.rightReporter = reporter;

        FileInputFormat leftFIF;
        try {
            leftFIF = (FileInputFormat) ReflectionUtils.newInstance(
                    Class.forName(conf.get(CartesianInputFormat.LEFT_INPUT_FORMAT)), conf);
            rightFIF = (FileInputFormat) ReflectionUtils.newInstance(
                    Class.forName(conf.get(CartesianInputFormat.RIGHT_INPUT_FORMAT)), conf);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        leftRR = leftFIF.getRecordReader(split.get(0), conf, reporter);
        rightRR = rightFIF.getRecordReader(rightIS, rightConf, rightReporter);

        lkey = (K1) this.leftRR.createKey();
        lvalue = (V1) this.leftRR.createValue();

        rkey = (K2) this.rightRR.createKey();
        rvalue = (V2) this.rightRR.createValue();
    }

    public boolean next(Text key, Text value) throws IOException {

        do {

            if (goToNextLeft) {
                if (!leftRR.next(lkey, lvalue)) {
                    alldone = true;
                    break;
                } else {
                    key.set(lvalue.toString());
                    goToNextLeft = alldone = false;

                    this.rightRR = this.rightFIF.getRecordReader(
                            this.rightIS, this.rightConf,
                            this.rightReporter);
                }
            }

            if (rightRR.next(rkey, rvalue)) {
                value.set(rvalue.toString());
            } else {
                goToNextLeft = true;
            }

        } while (goToNextLeft);

        return !alldone;
    }

    @Override
    public void close() throws IOException {
        leftRR.close();
        rightRR.close();
    }

    @Override
    public Text createKey() {
        return new Text();
    }

    @Override
    public Text createValue() {
        return new Text();
    }

    @Override
    public long getPos() throws IOException {
        return leftRR.getPos();
    }

    @Override
    public float getProgress() throws IOException {
        return leftRR.getProgress();
    }

}
