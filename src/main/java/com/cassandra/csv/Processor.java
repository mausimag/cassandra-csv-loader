package com.cassandra.csv;

import java.io.File;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import com.google.common.base.Joiner;
import org.joda.time.Days;
import org.joda.time.DateTime;


/**
 * Created by mauricio on 18/02/2016.
 */
public class Processor {
    public BigInteger defineBucket(String ds, String endStr) throws ParseException {
        BigInteger bucket;
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
        DateTime start = new DateTime(sf.parse("1970-01-01 12:00:00+0000"));
        DateTime end = new DateTime(sf.parse(endStr));
        bucket = new BigInteger(Days.daysBetween(start, end).getDays() + ds);
        return bucket;
    }

    public String applyRow(String line) {
        String[] vals = line.split(",");
        String module_time = vals[13];
        try {
            vals[0] = defineBucket("003", module_time).toString();
            return Joiner.on(", ").join(vals);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void processFile(Config config) {
        final Cassandra.Connection conn = new Cassandra.Connection();
        final String tablename = config.getTablename();
        final String columns = config.getColumns();
        conn.connect(config.getHost(), config.getKeyspace());

        CsvLoader csv = new CsvLoader();
        csv.open(config.getPath());

        csv.forEachLine(new Line() {
            @Override
            public void line(String ln) {
                String insertQuery = Cassandra.Query.buildInsert(tablename, columns, ln);
                conn.getSession().execute(applyRow(ln));
            }
        });

        conn.close();
    }

    public void processDirectory(final Config config) {
        File folder = new File(config.getPath());
        File[] listOfFiles = folder.listFiles();

        for (File listOfFile : listOfFiles) {
            if (listOfFile.isFile()) {
                final String path = listOfFile.getAbsolutePath();
                final Cassandra.Connection conn = new Cassandra.Connection();
                final String tablename = config.getTablename();
                final String columns = config.getColumns();
                conn.connect(config.getHost(), config.getKeyspace());
                CsvLoader csv = new CsvLoader();
                csv.open(path);

                csv.forEachLine(new Line() {
                    @Override
                    public void line(String ln) {
                        String updatedLine = applyRow(ln);
                        String insertQuery = Cassandra.Query.buildInsert(tablename, columns, updatedLine);
                        conn.getSession().execute(insertQuery);
                    }
                });
                conn.close();
            }
        }
    }
}
