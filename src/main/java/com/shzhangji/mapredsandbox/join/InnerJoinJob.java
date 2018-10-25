package com.shzhangji.mapredsandbox.join;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class InnerJoinJob extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Path orderInput = new Path("mapred-sandbox/data/join/order.csv");
        Path productInput = new Path("mapred-sandbox/data/join/product.csv");
        Path output = new Path("mapred-sandbox/output/join/inner-join");

        Job job = new Job(getConf());
        job.setJarByClass(InnerJoinJob.class);

        MultipleInputs.addInputPath(job, orderInput, TextInputFormat.class, OrderMapper.class);
        MultipleInputs.addInputPath(job, productInput, TextInputFormat.class, ProductMapper.class);
        FileOutputFormat.setOutputPath(job, output);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class OrderMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // skip the column line
            if (key.get() == 0) {
                return;
            }

            String[] columns = value.toString().split(",");
            context.write(new Text(columns[2]), new Text("Order:" + value.toString()));
        }

    }

    public static class ProductMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            if (key.get() == 0) {
                return;
            }

            String[] columns = value.toString().split(",");
            context.write(new Text(columns[0]), new Text("Product:" + value.toString()));
        }

    }

    public static class JoinReducer extends Reducer<Text, Text, NullWritable, Text> {

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
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            List<String> orders = new ArrayList<String>();
            List<String> products = new ArrayList<String>();

            for (Text value : values) {

                String valueString = value.toString();
                int pos = valueString.indexOf(":");
                String tag = valueString.substring(0, pos);

                if (tag.equals("Order")) {
                    orders.add(valueString.substring(pos + 1));
                } else if (tag.equals("Product")) {
                    products.add(valueString.substring(pos + 1));
                }

            }

            for (String orderString : orders) {
                String[] order = orderString.split(",");
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
            }

        }

    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new InnerJoinJob(), args));
    }

}
