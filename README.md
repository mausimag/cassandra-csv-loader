Cassandra CSV Loader
====================

Using CSV loader:
==================

    public static void main(String[] args)
    {
        Config config = new Config();
        config.parseArgs(args);
        
        Cassandra.Connection conn = new Cassandra.Connection();
        String tablename = config.getTablename();
        String columns = config.getColumns();
        conn.connect(config.getHost(), config.getKeyspace());
    
        CsvLoader csv = new CsvLoader();
        csv.open(config.getPath());
    
        csv.forEachLine(new Line()
        {
            /**
             *
             * @param line is the current line in csv
             */
            @Override
            public void line(String line)
            {
                // Build query
                String query = Cassandra.Query.buildInsert(tablename, columns, line);
                
                // Execute query
                conn.getSession().execute(query);
            }
        });
    
        conn.close();
    }


Calling csv loader
==================
Example single file:

    cassandra-csv-loader.jar --dir=/mydir/csv/vl_diagnostic.csv --host=127.0.0.1 --keyspace=sotreq --tablename=vl_diagnostic --columns=bucket:bigint,serialnumber:text,timestamp:timestamp,mid:int,cid:int,fmi:int,level:int,cassandra_insert_time:timestamp,make:text,mastermsgid:bigint,messageid:bigint,model:text,modulecode:text,moduletime:timestamp,nickname:text,occurrences:int,receivedtime:timestamp
    
    
Example directory (used for large csv):
    
Split file in small files:

    split large_csv.csv
        
Process small csvs:

    cassandra-csv-loader.jar --dir=/mydir/csv/ --host=127.0.0.1 --keyspace=sotreq --tablename=vl_diagnostic --columns=bucket:bigint,serialnumber:text,timestamp:timestamp,mid:int,cid:int,fmi:int,level:int,cassandra_insert_time:timestamp,make:text,mastermsgid:bigint,messageid:bigint,model:text,modulecode:text,moduletime:timestamp,nickname:text,occurrences:int,receivedtime:timestamp