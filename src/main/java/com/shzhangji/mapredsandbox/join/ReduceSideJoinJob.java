package com.shzhangji.mapredsandbox.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class ReduceSideJoinJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Path orderInput = new Path("mapred-sandbox/data/join/order.csv");
        Path productInput = new Path("mapred-sandbox/data/join/product.csv");
        Path output = new Path("mapred-sandbox/output/join/reduce-side-join");

        FileSystem fs = FileSystem.get(getConf());
        fs.delete(output, true);

        Job job = new Job(getConf());
        job.setJarByClass(ReduceSideJoinJob.class);

        MultipleInputs.addInputPath(job, orderInput, TextInputFormat.class, OrderMapper.class);
        MultipleInputs.addInputPath(job, productInput, TextInputFormat.class, ProductMapper.class);
        FileOutputFormat.setOutputPath(job, output);

        job.setMapOutputKeyClass(TaggedKey.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(TaggedJoiningPartitioner.class);
        job.setGroupingComparatorClass(TaggedJoiningGroupingComparator.class);

        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class OrderMapper extends Mapper<LongWritable, Text, TaggedKey, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // skip the column line
            if (key.get() == 0) {
                return;
            }

            String[] columns = value.toString().split(",");
            TaggedKey taggedKey = new TaggedKey();
            taggedKey.set(columns[2], 2); // last table
            context.write(taggedKey, value);
        }

    }

    public static class ProductMapper extends Mapper<LongWritable, Text, TaggedKey, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            if (key.get() == 0) {
                return;
            }

            String[] columns = value.toString().split(",");
            TaggedKey taggedKey = new TaggedKey();
            taggedKey.set(columns[0], 1);
            context.write(taggedKey, value);
        }

    }

    public static class JoinReducer extends Reducer<TaggedKey, Text, NullWritable, Text> {

        private NumberFormat nf;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {

            nf = NumberFormat.getNumberInstance();
            nf.setMinimumFractionDigits(2);
            nf.setMaximumFractionDigits(2);

            context.write(NullWritable.get(), new Text("order_id,user_id,product_id,product_name,unit_price,count,amount"));
        }

        @Override
        protected void reduce(TaggedKey key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            List<String> products = new ArrayList<String>();

            for (Text value : values) {

                switch (key.getTag().get()) {
                case 1: // product
                    products.add(value.toString());
                    break;

                case 2: // order

                    String[] order = value.toString().split(",");

                    for (String productString : products) {

                        String[] product = productString.split(",");

                        List<String> output = new ArrayList<String>();
                        output.add(order[0]);
                        output.add(order[1]);
                        output.add(order[2]);
                        output.add(product[1]);
                        output.add(product[2]);
                        output.add(order[3]);
                        output.add(nf.format(Double.parseDouble(product[2]) * Long.parseLong(order[3])));

                        context.write(NullWritable.get(), new Text(StringUtils.join(output, ",")));
                    }

                    break;

                default:
                    assert false;
                }

            }

        }

    }

    public static class TaggedKey implements WritableComparable<TaggedKey> {

        private Text joinKey = new Text();
        private IntWritable tag = new IntWritable();

        @Override
        public int compareTo(TaggedKey taggedKey) {
            int compareValue = joinKey.compareTo(taggedKey.getJoinKey());
            if (compareValue == 0) {
                compareValue = tag.compareTo(taggedKey.getTag());
            }
            return compareValue;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            joinKey.readFields(in);
            tag.readFields(in);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            joinKey.write(out);
            tag.write(out);
        }

        public void set(String joinKey, int tag) {
            this.joinKey.set(joinKey);
            this.tag.set(tag);
        }

        public Text getJoinKey() {
            return joinKey;
        }

        public IntWritable getTag() {
            return tag;
        }

    }

    public static class TaggedJoiningPartitioner extends Partitioner<TaggedKey, Text> {

        @Override
        public int getPartition(TaggedKey taggedKey, Text text, int numPartitions) {
            return taggedKey.getJoinKey().hashCode() % numPartitions;
        }

    }

    public static class TaggedJoiningGroupingComparator extends WritableComparator {

        public TaggedJoiningGroupingComparator() {
            super(TaggedKey.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            TaggedKey taggedKey1 = (TaggedKey) a;
            TaggedKey taggedKey2 = (TaggedKey) b;
            return taggedKey1.getJoinKey().compareTo(taggedKey2.getJoinKey());
        }

    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ReduceSideJoinJob(), args));
    }

}
