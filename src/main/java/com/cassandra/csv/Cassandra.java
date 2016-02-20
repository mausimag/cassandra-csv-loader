package com.cassandra.csv;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.sun.org.apache.xpath.internal.SourceTree;

/**
 * Created by mauricio on 18/02/2016.
 */
public class Cassandra
{
    public static class Connection
    {
        private Cluster cluster = null;
        private Session session = null;

        public void connect(String host, String keyspace)
        {
            cluster = Cluster.builder().addContactPoint(host).build();
            session = cluster.connect(keyspace);
        }

        public Session getSession()
        {
            return session;
        }


        public void close()
        {
            cluster.close();
            session.close();
        }
    }


    public static class Query
    {
        public static String parseColumn (String column, String value){
            if (column.equals("text") || column.equals("varchar")|| column.equals("timestamp")) {
                value = "'" + value + "'";
            }
            return value;
        }

        public void updateColumn()
        {

        }

        public static String buildInsert(String tablename, String columns, String line)
        {
            String values[] = line.split(",");
            String cols[] = columns.split(",");
            String valuesStr = "";
            String colsStr = "";

            if ( values.length != cols.length) {
                System.out.println(values.length +"-"+ cols.length);
                System.exit(1);
            }

            for (int i = 0; i < values.length; i++) {
                valuesStr += parseColumn(cols[i].split(":")[1], values[i]);
                colsStr += cols[i].split(":")[0];
                valuesStr += ",";
                colsStr += ",";
            }

            colsStr = colsStr.substring(0, colsStr.length()-1);
            valuesStr = valuesStr.substring(0, valuesStr.length()-1);

            return String.format("INSERT INTO %s (%s) VALUES (%s);", tablename, colsStr, valuesStr);
        }
    }
}
