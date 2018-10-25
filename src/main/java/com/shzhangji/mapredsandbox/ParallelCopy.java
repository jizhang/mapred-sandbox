package com.shzhangji.mapredsandbox;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ParallelCopy extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        ExecutorService service = Executors.newFixedThreadPool(5);
        List<Future<Void>> futures = new ArrayList<Future<Void>>();

        FileSystem fs = FileSystem.get(getConf());

        for (FileStatus file :fs.globStatus(new Path(args[0]))) {

            final Path path = file.getPath();
            final File outFile = new File(args[1], path.getName());

            futures.add(service.submit(new Callable<Void>() {

                @Override
                public Void call() throws Exception {

                    InputStream in = FileSystem.get(getConf()).open(path);
                    OutputStream out = new FileOutputStream(outFile);

                    byte[] buf = new byte[8192];
                    int len;
                    while ((len = in.read(buf)) > 0) {
                        out.write(buf, 0, len);
                    }
                    in.close();
                    out.close();

                    System.out.println("Written " + path.getName());

                    return null;
                }

            }));

        }

        try {
            for (Future<Void> future : futures) {
                future.get();
            }
        } finally {
            service.shutdown();
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ParallelCopy(), args));
    }

}
