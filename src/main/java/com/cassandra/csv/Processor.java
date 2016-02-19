package com.cassandra.csv;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by mauricio on 18/02/2016.
 */
public class Processor
{
    private ExecutorService ex = Executors.newFixedThreadPool(10);

    public void processFile(Config config)
    {
        final Cassandra conn = new Cassandra();
        final String tablename = config.getTablename();
        final String columns = config.getColumns();
        conn.connect(config.getHost(), config.getKeyspace());

        CsvLoader csv = new CsvLoader();
        csv.open(config.getPath());

        csv.forEachLine(new Line() {
            @Override
            public void line(String ln) {
                String query = Cassandra.Query.buildInsert(tablename, columns, ln);
                conn.getSession().execute(query);
            }
        });

        conn.close();
    }

    public void processDirectory(final Config config)
    {
        File folder = new File(config.getPath());
        File[] listOfFiles = folder.listFiles();

        for (int i = 0; i < listOfFiles.length; i++)
        {
            if (listOfFiles[i].isFile())
            {
                final String path = listOfFiles[i].getAbsolutePath();
                ex.submit(new Runnable() {
                    @Override
                    public void run() {
                        final Cassandra conn = new Cassandra();
                        final String tablename = config.getTablename();
                        final String columns = config.getColumns();
                        conn.connect(config.getHost(), config.getKeyspace());
                        CsvLoader csv = new CsvLoader();
                        csv.open(path);

                        csv.forEachLine(new Line() {
                            @Override
                            public void line(String ln) {
                                String query = Cassandra.Query.buildInsert(tablename, columns, ln);
                                conn.getSession().execute(query);
                            }
                        });
                        conn.close();
                    }
                });
            }
        }
    }
}
