package com.cassandra.csv;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * Created by mauricio on 18/02/2016.
 */
public class ConnectionFactory
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
