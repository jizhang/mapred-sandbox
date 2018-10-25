package com.shzhangji.mapredsandbox;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AutoIncrementId2Job extends Configured implements Tool {

    @Override
    public int run(String[] arg0) throws Exception {

        Path input = new Path("mapred-sandbox/data/auto-increment-id/");
        Path output = new Path("mapred-sandbox/output/auto-increment-id/");
        Path temp = new Path("mapred-sandbox/output/auto-increment-id-temp/");
        Path partition = new Path("mapred-sandbox/output/auto-increment-id-partition");

        FileSystem fs = FileSystem.get(getConf());

        // write partition file
        SequenceFile.Writer writer = new SequenceFile.Writer(fs, getConf(), partition,
                Text.class, NullWritable.class);
        Text key = new Text();
        NullWritable value = NullWritable.get();
        key.set("12:00");
        writer.append(key, value);
        key.set("13:00");
        writer.append(key, value);
        writer.close();

        // job A
        fs.delete(temp, true);
        Job job = new Job(getConf());
        job.setJarByClass(AutoIncrementId2Job.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, temp);

        job.setMapperClass(JobMapperA.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(JobReducer.class);
        job.setNumReduceTasks(3);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(TotalOrderPartitioner.class);
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partition);

        MultipleOutputs.addNamedOutput(job, "count", SequenceFileOutputFormat.class,
                NullWritable.class, LongWritable.class);

        job.waitForCompletion(true);

        // job B
        fs.delete(output, true);
        job = new Job(getConf());
        job.setJarByClass(AutoIncrementId2Job.class);

        job.setInputFormatClass(NonSplitableSequence.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, temp);
        FileOutputFormat.setOutputPath(job, output);

        job.setMapperClass(JobMapperB.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(0);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        // pass count info
        job.getConfiguration().set("startIds", Base64.encodeBase64String(
                SerializationUtils.serialize((Serializable) getStartIds(temp))));

        job.waitForCompletion(true);

        return 0;
    }

    private Map<String, Long> getStartIds(Path countPath) throws Exception {

        Map<String, Long> startIds = new HashMap<String, Long>();
        long startId = 1;
        FileSystem fs = FileSystem.get(getConf());
        for (FileStatus file : fs.listStatus(countPath)) {

            Path path = file.getPath();
            String name = path.getName();
            if (!name.startsWith("count-")) {
                continue;
            }

            startIds.put(name.substring(name.length() - 5), startId);

            SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, getConf());
            NullWritable key = NullWritable.get();
            LongWritable value = new LongWritable();
            if (!reader.next(key, value)) {
                continue;
            }
            startId += value.get();
            reader.close();
        }

        return startIds;

    }

    public static class NonSplitableSequence extends SequenceFileInputFormat<NullWritable, Text> {

        @Override
        protected boolean isSplitable(JobContext context, Path filename) {
            return false;
        }

    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new AutoIncrementId2Job(), args));
    }

    public static class JobMapperA extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] segs = value.toString().split("\\s+");
            context.write(new Text(segs[0]), value);
        }

    }

    public static class JobReducer extends Reducer<Text, Text, NullWritable, Text> {

        private MultipleOutputs<NullWritable, Text> mos;
        private long count;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {

            super.setup(context);
            mos = new MultipleOutputs<NullWritable, Text>(context);
            count = 0;
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text value : values) {
                context.write(NullWritable.get(), value);
                ++count;
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            super.cleanup(context);
            mos.write("count", NullWritable.get(), new LongWritable(count));
            mos.close();
        }

    }

    public static class JobMapperB extends Mapper<NullWritable, Text, LongWritable, Text> {

        private Map<String, Long> startIds;
        private long startId;

        @SuppressWarnings("unchecked")
        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {

            super.setup(context);
            startIds = (Map<String, Long>) SerializationUtils.deserialize(
                    Base64.decodeBase64(context.getConfiguration().get("startIds")));
            String name = ((FileSplit) context.getInputSplit()).getPath().getName();
            startId = startIds.get(name.substring(name.length() - 5));
        }

        @Override
        protected void map(NullWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            context.write(new LongWritable(startId++), value);
        }

    }

}
