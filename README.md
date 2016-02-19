Cassandra CSV Loader
====================

Example single file:

    cassandra-csv-loader-1.0-SNAPSHOT.jar --dir=/mydir/csv/vl_diagnostic.csv --host=127.0.0.1 --keyspace=sotreq --tablename=vl_diagnostic --columns=bucket:bigint,serialnumber:text,timestamp:timestamp,mid:int,cid:int,fmi:int,level:int,cassandra_insert_time:timestamp,make:text,mastermsgid:bigint,messageid:bigint,model:text,modulecode:text,moduletime:timestamp,nickname:text,occurrences:int,receivedtime:timestamp
    
    
Example directory (used for large csv):
    
Split file in small files:

        split large_csv.csv
        
Process small csvs:

        cassandra-csv-loader-1.0-SNAPSHOT.jar --dir=/mydir/csv/ --host=127.0.0.1 --keyspace=sotreq --tablename=vl_diagnostic --columns=bucket:bigint,serialnumber:text,timestamp:timestamp,mid:int,cid:int,fmi:int,level:int,cassandra_insert_time:timestamp,make:text,mastermsgid:bigint,messageid:bigint,model:text,modulecode:text,moduletime:timestamp,nickname:text,occurrences:int,receivedtime:timestamp