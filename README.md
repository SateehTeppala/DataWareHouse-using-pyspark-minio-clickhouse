# DataWareHouse-using-pyspark-minio-clickhouse
DataWareHouse-using-pyspark-minio-clickhouse

requirements

- download `aws-java-sdk-bundle-1.12.782.jar` from maven repo and place it into `jars/`
- install minio  [Guide](https://min.io/docs/minio/macos/index.html)
   - create two buckets web3 and web33 i.e, web3 for the dump and web33 for the warehouse 
- install click house [Guide](https://clickhouse.com/docs/install)
- install dbeaver optional -> to run sql queries on clickhouse

  

## ETL Stage: 

1. git clone the repo
2. run ` pip install -r requirements.txt`
3. ingestDataintoMinio.py -> for ingest api data into minio buckets as csv file
4. etlPySpark.py -> for the etl stage and load the data as a delta table in minio [delta docs](https://docs.delta.io/latest/index.html)
5. check minio bucket to see the populated data


## data engine with click house 

1. start clickhouse with ```./clickhouse server`
2. open dbeaver and connect to clickhouse which runs on 8123 or start `./clickhouse client` to the sql queries
3. create a database `CREATE database myschema; `
4. run this to create a table in clickhouse [guide](https://clickhouse.com/docs/engines/table-engines/integrations/deltalake)
```sql
   CREATE TABLE myschema.ABCD0TABLE ENGINE=DeltaLake('http://127.0.0.1:9000/web33/DataLake/tables/ABCD0TABLE', 'minioadmin', 'minioadmin');
   ```
5.  to see the data
```sql
   SELECT * FROM myschema.ABCD0TABLE;
```
   
