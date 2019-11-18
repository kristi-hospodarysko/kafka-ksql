DROP TABLE iot_aggregated_usage;
DROP STREAM iot_joined_records;
DROP TABLE iot_devices;
DROP STREAM iot_usage_records;

CREATE STREAM iot_usage_records
  (id VARCHAR,
   device_sn VARCHAR,
   download_bytes BIGINT,
   upload_bytes BIGINT,
   session_start_time BIGINT)
 WITH (KAFKA_TOPIC='iot_usage_records',
       VALUE_FORMAT='JSON',
       KEY = 'id');

CREATE TABLE iot_devices
  (id VARCHAR,
   serial_number VARCHAR,
   account_sn VARCHAR)
  WITH (KAFKA_TOPIC = 'IOT_DEVICES',
        VALUE_FORMAT='JSON',
        KEY = 'serial_number');

CREATE STREAM iot_joined_records WITH (PARTITIONS=1, VALUE_FORMAT='JSON') AS \
SELECT R.device_sn as device_sn, D.account_sn, R.download_bytes, R.upload_bytes, R.session_start_time \
FROM iot_usage_records R \
INNER JOIN iot_devices D \
ON D.serial_number = R.device_sn;

CREATE TABLE iot_aggregated_usage WITH (PARTITIONS=1, VALUE_FORMAT='JSON') AS \
SELECT account_sn, 
	SUM(download_bytes) as download_bytes, 
	SUM(upload_bytes) as upload_bytes, 
	TIMESTAMPTOSTRING(WindowStart(), 'yyyy-MM-dd HH:mm:ss') as period_start, 
	TIMESTAMPTOSTRING(WindowEnd(), 'yyyy-MM-dd HH:mm:ss') as period_end
FROM iot_joined_records \
     WINDOW TUMBLING (SIZE 1 MINUTES) \
GROUP BY account_sn;
