package com.shzhangji.mapredsandbox.filtering;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

public class BloomFilterDriver {

    public static void main(String[] args) throws Exception {

        // Parse command line arguments
        Path inputFile = new Path(args[0]);
        int numMembers = Integer.parseInt(args[1]);
        float falsePosRate = Float.parseFloat(args[2]);
        Path bfFile = new Path(args[3]);

        // Calculate our vector size and optimal K value based on approximations
        int vectorSize = getOptimalBloomFilterSize(numMembers, falsePosRate);
        int nbHash = getOptimalK(numMembers, vectorSize);

        // Create new Bloom filter
        BloomFilter filter = new BloomFilter(vectorSize, nbHash, Hash.MURMUR_HASH);
        System.out.println("Training Bloom filter of size " + vectorSize
                + " with " + nbHash + " hash functions, " + numMembers
                + " approximate number of records, and " + falsePosRate
                + " false positive rate");

        // Open file for read
        String line = null;
        int numElements = 0;
        FileSystem fs = FileSystem.get(new Configuration());
        for (FileStatus status : fs.listStatus(inputFile)) {
            BufferedReader rdr = new BufferedReader(new InputStreamReader(
                    new GZIPInputStream(fs.open(status.getPath()))));
            System.out.println("Reading " + status.getPath());
            while ((line = rdr.readLine()) != null) {
                filter.add(new Key(line.getBytes()));
                ++numElements;
                rdr.close();
            }
        }
        System.out.println("Trained Bloom filter with " + numElements
                + " entries.");
        System.out.println("Serializing Bloom filter to HDFS at " + bfFile);
        FSDataOutputStream strm = fs.create(bfFile);
        filter.write(strm);
        strm.flush();
        strm.close();
        System.exit(0);
    }

    public static int getOptimalBloomFilterSize(int numElements,
            float falsePosRate) {

        return (int) (-numElements * (float) Math.log(falsePosRate) / Math.pow(
                Math.log(2), 2));
    }

    public static int getOptimalK(float numElements, float vectorSize) {
        return (int) Math.round(vectorSize * Math.log(2) / numElements);
    }

}
