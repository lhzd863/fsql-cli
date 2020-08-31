# fsql-cli

# cmd
```
$FLINK_DIR/bin/flink run -c com.fsql.client.SqlCli -d -p 3 ${STREAM_HOME}/lib/fsql-cli-0.0.1.jar -f "$1" -j "$2"

```

# script
```
sql="
--flink
CREATE TABLE tb1 (
    id STRING,
    log_ts STRING
,proctime as PROCTIME()
) WITH (
    'connector' = 'kafka', 
    'topic' = 'xxx',
    'properties.group.id' = 'xxx',
    'properties.bootstrap.servers' = 'xxx:9092',
    'format' = 'json',
    'scan.startup.mode' = 'group-offsets'
);

--flink
CREATE TABLE tb2 (
  id string,
  log_ts string,
  PRIMARY KEY (id,log_ts) NOT ENFORCED
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://xxx:3306/xx',
   'table-name' = 'xxx',
   'username' = 'test',
   'password' = 'test',
   'sink.buffer-flush.interval' = '10s'
);
--flink
INSERT INTO tb2
select
 id
,log_ts
from
 tb1
;
"
bash $STREAM_HOME/bin/run.sh "$sql" xxx_realtime
```
