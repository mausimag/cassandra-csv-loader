package com.cassandra.csv;

import java.io.File;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by mauricio on 18/02/2016.
 */
public class Processor
{
    private ExecutorService ex = Executors.newFixedThreadPool(10);

    private void process(Runnable r)
    {
        ex.execute(r);
    }

    public void processFile(Config config)
    {
        final ConnectionFactory conn = new ConnectionFactory();
        conn.connect(config.getHost(), config.getKeyspace());

        CsvLoader csv = new CsvLoader();
        csv.open(config.getPath());

        csv.forEachLine(new Line() {
            @Override
            public void line(String ln) {
                String line[] = {"a" + new Random().nextInt(), "2", "3", "4", "5"};
                String query = String.format("insert into teste (coluna1, coluna2, coluna3, coluna4, coluna5) " + "values ('%s', '%s', '%s', '%s', '%s');", line[0], line[1], line[2], line[3], line[4]);
                conn.getSession().execute(query);
                System.out.println(query);
            }
        });

        conn.close();
    }

    public void processDirectory(Config config)
    {
        File folder = new File(config.getPath());
        File[] listOfFiles = folder.listFiles();

        assert listOfFiles != null;
        for (int i = 0; i < listOfFiles.length; i++)
        {
            if (listOfFiles[i].isFile())
            {
                config.setPath(listOfFiles[i].getAbsolutePath());
                processFile(config);
            }
        }
    }
}
