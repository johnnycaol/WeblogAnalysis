--REGISTER /Users/johnnycao/.m2/repository/org/apache/pig/piggybank/0.16.0/piggybank-0.16.0.jar
--REGISTER /Users/johnnycao/.m2/repository/com/linkedin/datafu/datafu/1.2.0/datafu-1.2.0.jar
DEFINE ISOToUnix org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();
DEFINE MyRegExLoader org.apache.pig.piggybank.storage.MyRegExLoader('([^ ]*) ([^ ]*) ([^ ]*):([0-9]*) ([^ ]*):([0-9]*) ([.0-9]*) ([.0-9]*) ([.0-9]*) (-|[0-9]*) (-|[0-9]*) ([-|0-9]*) ([-|0-9]*) \\"([^ ]*) ([^ ]*) (- |[^ ]*)\\" \\"((?:[^\\"]|\\")+)\\" ([^ ]*) ([^ ]*)$');
DEFINE Sessionize datafu.pig.sessions.Sessionize('15m');
DEFINE Quantile datafu.pig.stats.StreamingQuantile('0.95');--95th percentile

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

-- Calculate session length in seconds
session_length = FOREACH (GROUP sessionized_logs BY (session_id, request_ip)) {
   GENERATE group.session_id AS session_id,
       group.request_ip AS request_ip,
        ((MAX(sessionized_logs.access_timestamp) - MIN(sessionized_logs.access_timestamp)) / 1000) AS session_length;
};

-- Calculate the most engaged users
quantile_session_length = FOREACH (GROUP session_length ALL) {
 GENERATE Quantile(session_length.session_length) AS quantile_session_length;
};

long_sessions = FILTER session_length BY
 session_length >= quantile_session_length.quantile_session_length.quantile_0_95;

most_engaged_users = DISTINCT (FOREACH long_sessions GENERATE request_ip);

STORE most_engaged_users INTO 'most_engaged_users.out';
