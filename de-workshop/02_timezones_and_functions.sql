-- List of files in stage 
list @uni_kishore/kickoff;

-- Create new file format for json 
create file format FF_JSON_LOGS
    type = JSON
    strip_outer_array = true;

-- Show values from stage in file format 
select $1 
from @uni_kishore/kickoff
(file_format => ff_json_logs);

select $1 
from @uni_kishore/updated_feed
(file_format => ff_json_logs);

-- Load the File Into The Table from stages (if file name is not specified, Snowflake will attempt to load all the files in the stage (or the stage/folder location)) 
copy into ags_game_audience.raw.GAME_LOGS 
from @uni_kishore/kickoff
file_format = (format_name = FF_JSON_LOGS);

select * from ags_game_audience.raw.GAME_LOGS;

copy into ags_game_audience.raw.GAME_LOGS 
from @uni_kishore/updated_feed
file_format = (format_name = FF_JSON_LOGS);

select * from ags_game_audience.raw.GAME_LOGS;

-- Separate every attribute into its own column and create a view 
select 
    RAW_LOG:agent::text as AGENT 
    , RAW_LOG:user_event::text as USER_EVENT
    , RAW_LOG:datetime_iso8601::TIMESTAMP_NTZ as DATETIME_ISO8601    
    , RAW_LOG:user_login::text as USER_LOGIN
    , RAW_LOG:ip_address::text as IP_ADDRESS
    , * -- the original column 
from GAME_LOGS;

-- Separate every attribute into its own column and create a view 
create view AGS_GAME_AUDIENCE.RAW.LOGS as
    select 
        RAW_LOG:agent::text as AGENT 
        , RAW_LOG:user_event::text as USER_EVENT
        , RAW_LOG:datetime_iso8601::TIMESTAMP_NTZ as DATETIME_ISO8601    
        , RAW_LOG:user_login::text as USER_LOGIN
        , * -- the original column 
    from GAME_LOGS;

    -- see the view 
select * from AGS_GAME_AUDIENCE.RAW.LOGS;

--looking for empty AGENT column
select * 
from ags_game_audience.raw.LOGS
where agent is null;

--looking for non-empty IP_ADDRESS column
select 
RAW_LOG:ip_address::text as IP_ADDRESS
,*
from ags_game_audience.raw.LOGS
where RAW_LOG:ip_address::text is not null;

-- Change the view 
create or replace view AGS_GAME_AUDIENCE.RAW.LOGS as
    select 
        RAW_LOG:ip_address::text as IP_ADDRESS 
        , RAW_LOG:user_event::text as USER_EVENT
        , RAW_LOG:datetime_iso8601::TIMESTAMP_NTZ as DATETIME_ISO8601    
        , RAW_LOG:user_login::text as USER_LOGIN
        , * -- the original column 
    from GAME_LOGS
    where RAW_LOG:ip_address::text is not null;

select * from AGS_GAME_AUDIENCE.RAW.LOGS;

select * from AGS_GAME_AUDIENCE.RAW.LOGS WHERE USER_LOGIN ilike '%Prajina%';

-- Use parse_ip function to find IP details 
select parse_ip('100.41.16.160','inet');
-- Show fileds from parse_ip 
select parse_ip('100.41.16.160','inet'):ipv4;

--Look up Kishore and Prajina's Time Zone in the IPInfo share using his headset's IP Address with the PARSE_IP function.
select start_ip, end_ip, start_ip_int, end_ip_int, city, region, country, timezone
from IPINFO_GEOLOC.demo.location
where parse_ip('100.41.16.160', 'inet'):ipv4 --Kishore's Headset's IP Address
BETWEEN start_ip_int AND end_ip_int;

--Join the log and location tables to add time zone to each row using the PARSE_IP function.
select logs.*
       , loc.city
       , loc.region
       , loc.country
       , loc.timezone
from AGS_GAME_AUDIENCE.RAW.LOGS logs
join IPINFO_GEOLOC.demo.location loc
where parse_ip(logs.ip_address, 'inet'):ipv4 
BETWEEN start_ip_int AND end_ip_int;

--Use two functions supplied by IPShare to help with an efficient IP Lookup Process!
SELECT logs.ip_address, 
, logs.user_login
, logs.user_event
, logs.datetime_iso8601
, city
, region
, country
, timezone 
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int;


-- 02. Time zones

-- find current 
select current_timestamp();

--worksheets are sometimes called sessions -- we'll be changing the worksheet time zone
alter session set timezone = 'UTC';
select current_timestamp();

--how did the time differ after changing the time zone for the worksheet?
-- Snowflake uses IANA list: https://data.iana.org/time-zones/tzdb-2021a/zone1970.tab
alter session set timezone = 'Africa/Nairobi';
select current_timestamp();

alter session set timezone = 'Pacific/Funafuti';
select current_timestamp();

alter session set timezone = 'Asia/Shanghai';
select current_timestamp();

--show the account parameter called timezone
show parameters like 'timezone';
