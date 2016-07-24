--REGISTER /Users/johnnycao/.m2/repository/org/apache/pig/piggybank/0.16.0/piggybank-0.16.0.jar
--REGISTER /Users/johnnycao/.m2/repository/com/linkedin/datafu/datafu/1.2.0/datafu-1.2.0.jar
DEFINE MyRegExLoader org.apache.pig.piggybank.storage.MyRegExLoader('([^ ]*) ([^ ]*) ([^ ]*):([0-9]*) ([^ ]*):([0-9]*) ([.0-9]*) ([.0-9]*) ([.0-9]*) (-|[0-9]*) (-|[0-9]*) ([-|0-9]*) ([-|0-9]*) \\"([^ ]*) ([^ ]*) (- |[^ ]*)\\" \\"((?:[^\\"]|\\")+)\\" ([^ ]*) ([^ ]*)$');
DEFINE ISOToUnix org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();
DEFINE Sessionize datafu.pig.sessions.Sessionize('15m');

-- Load the data
logs = LOAD '$input'
USING MyRegExLoader() 
AS (
     access_timestamp: chararray,
    elb_name: chararray,
    request_ip: chararray,
    request_port: chararray,
    backend_ip: chararray,
    backend_port: chararray,
    request_processing_time: chararray,
    backend_processing_time: chararray,
    response_processing_time: chararray,
    elb_status_code: chararray,
    backend_status_code: chararray,
    received_bytes: chararray,
    sent_bytes: chararray,
    request: chararray,
    url: chararray,
    protocol: chararray,
    user_agent: chararray, 
    ssl_cipher: chararray,
    ssl_protocol: chararray
);

-- Remove if access_timestamp = null, then convert to Unix timestamp
logs = FOREACH (FILTER logs BY access_timestamp IS NOT null) {
    GENERATE ISOToUnix(access_timestamp) AS access_timestamp,
        elb_name,
        request_ip,
        request_port,
        backend_ip,
        backend_port,
        request_processing_time,
        backend_processing_time,
        response_processing_time,
        elb_status_code,
        backend_status_code,
        received_bytes,
        sent_bytes,
        request,
        url,
        protocol,
        user_agent, 
        ssl_cipher,
        ssl_protocol;
};

-- Sessionize all logs (attach session id to the end of each row based on the interval, e.g. 15 minutes)
sessionized_logs = FOREACH (GROUP logs BY request_ip) {
 ordered_logs = ORDER logs BY access_timestamp ASC;
 GENERATE FLATTEN(Sessionize(ordered_logs)) AS 
      (access_timestamp,
    elb_name,
    request_ip,
    request_port,
    backend_ip,
    backend_port,
    request_processing_time,
    backend_processing_time,
    response_processing_time,
    elb_status_code,
    backend_status_code,
    received_bytes,
    sent_bytes,
    request,
    url,
    protocol,
    user_agent, 
    ssl_cipher,
    ssl_protocol,
    session_id);
};

-- Calculate unique url count
unique_url_count = FOREACH (GROUP sessionized_logs BY session_id) { 
   unique_url = DISTINCT sessionized_logs.url;
   GENERATE group AS session_id,
       COUNT(unique_url) as unique_url_count;
};

-- Calculate average url count -> 8.31
avg_unique_url_count = FOREACH (GROUP unique_url_count ALL) {
 GENERATE ROUND_TO(AVG(unique_url_count.unique_url_count), 2) AS avg_unique_url_count;
};

STORE avg_unique_url_count INTO 'avg_unique_url_count.out';
