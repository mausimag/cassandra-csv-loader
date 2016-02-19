package com.cassandra.csv;

import java.io.File;
import java.util.Arrays;

/**
 * Created by mauricio on 18/02/2016.
 */
public class Main
{
    public static void main(String[] args)
    {
        Processor processor = new Processor();
        Config config = new Config();
        config.parseArgs(args);

        long startTime = System.nanoTime();

        if (new File(config.getPath()).isDirectory())
        {
            processor.processDirectory(config);
        } else
        {
            processor.processFile(config);
        }

        long stopTime = System.nanoTime();
        double seconds = (double) (stopTime - startTime) / 1000000000.0;
        System.out.println(seconds);
    }
}

