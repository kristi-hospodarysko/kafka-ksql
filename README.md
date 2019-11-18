# Home task

1. Create iot_usage_records topic that will contain json records with next structure:

     - id : uuid
     - device_sn : String // device serial number
     - download_bytes : Long
     - upload_bytes : Long
     - session_start_time : Long(epoch millis) // the timestamp of the network session start

 The iot device publish network usage in realtime to that topic. (You can have a script that emulates that)

2. Create iot_devices topic that will contain json records with next structure:

     - id : uuid
     - serial_number : string
     - account_sn : string // account serial number

This is a topic that persists iot devices forever.
In reality the producer could be a kafka-connector that dumps data from the Database.
(You can have a script that loads a set of devices to iot_devices topic).

3. Write a KSQL statement that aggregates total download_bytes and total upload_bytes
per account in a 1 minute window and publishes the aggregated usage to iot_aggregated_usage stream.
Usage records that don't belong to a valid device from iot_devices topic should be skipped.
The record json format:

 - account_sn : String
 - period_start : String (Window start)
 - period_end : String (Window end)
 - download_bytes : Long
 - upload_bytes : Long
