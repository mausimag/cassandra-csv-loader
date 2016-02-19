package com.cassandra.csv;

import org.joda.time.DateTime;
import org.joda.time.Days;

import java.io.File;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
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

    /*public BigInteger updateBucket(String value, DateTime end) {
        if(value == null) return BigInteger.valueOf(0L);

        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
        DateTime start = new DateTime(sf.parse("1970-01-01 12:00:00+0000"));
        BigInteger.apply(Days.daysBetween(start, end).getDays().toString + value);
    }*/

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
                                String vals[] = ln.split(",");
                                String strBucket = vals[0];
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
