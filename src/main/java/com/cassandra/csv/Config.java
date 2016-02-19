package com.cassandra.csv;

/**
 * Created by radix on 2/19/16.
 */
public class Config {
    private String path;
    private String host;
    private String keyspace;
    private String tablename;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public String getTablename() {
        return tablename;
    }

    public void setTablename(String tablename) {
        this.tablename = tablename;
    }

    public void parseArgs(String[] args)
    {
        for (String arg : args) {
            String[] p = arg.split("=");
            switch (p[0])
            {
                case "--dir":
                    setPath(p[1]);
                    break;
                case "--host":
                    setHost(p[1]);
                    break;
                case "--keyspace":
                    setKeyspace(p[1]);
                    break;
                case "--table":
                    setTablename(p[1]);
                    break;
            }
        }
    }
}
