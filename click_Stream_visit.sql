#1.创建visit模型
create external table click_stream_visit(
session string,
ip string,
timestr string,
request_url string,
setp string,
stayLong string,
from_url string,
platform string,
byte string,
status string
) partitioned by(datestr string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
#导入数据
load data inpath '/log/output/part-r-0000*' into table click_stream_visit partition(datestr='2012-01-04');


#2.创建贴源表
drop table if exists ods_click_pageviews;
create table ods_click_pageviews(
session string,
remote_addr string,
time_local string,
request string,
status string,
body_bytes_sent string,
http_referer string,
http_user_agent string,
step string,
stayLong string
)partitioned by (datestr string)
row format delimited
fields terminated by ',';
insert into table ods_click_pageviews partition(datestr='2012-01-04') select session,ip,timestr,request_url,status,byte,from_url,platform,setp,stayLong from click_stream_visit; 
#建立时间维度表
drop table dim_time if exists dim_time;
create table dim_time(
year string,
month string,
day string,
hour string)
row format delimited
fields terminated by ',';
#建立浏览器维度表
create table dim_browser(
browser string
);
#建立终端维度表
create table dim_os(
os string
);
##创建地域维度
create table dim_region(
province string,
city string
);

#3.ETL实现
#建立明细表
drop table ods_weblog_detail;
create table ods_weblog_detail(
remote_addr     string, --来源IP
remote_user     string, --用户标识
time_local      string, --访问完整时间
daystr          string, --访问日期
timestr         string, --访问时间
yearstr         string,	--访问年
month           string, --访问月
day             string, --访问日
hour            string, --访问时
request         string, --请求的url
status          string, --响应码
body_bytes_sent string, --传输字节数
http_referer    string, --来源url
ref_host        string, --来源的host
ref_path        string, --来源的路径
ref_query       string, --来源参数query
http_user_agent string,--客户终端标识
os  string,			   ---操作系统
province string,
city string )partitioned by(datestr string) row format delimited
fields terminated by ',';


#创建自己的函数，详见工程源码
add jar /home/hadoop/logAnalyzeHelper.jar;
#创建临时函数
create temporary function getOS as 'cn.edu.hust.udf.OSUtils';
create temporary function getBrowser as 'cn.edu.hust.udf.BrowserUtils';
create temporary function getProvince as 'cn.edu.hust.udf.IPUtils';
create temporary function getCity as 'cn.edu.hust.udf.CityUtils';


#导入操作系统维度表
insert into dim_os select distinct getOS(http_user_agent) from ods_click_pageviews;
#导入浏览器维度表
insert into dim_browser select distinct getBrowser(http_user_agent) from ods_click_pageviews;
##导入维度数据
insert into dim_region (city,province)
select distinct a.city as city,a.province as province
from ods_weblog_detail a
join (select distinct province from ods_weblog_detail) b
on a.province=b.province
where a.datestr='2012-01-04';

#导入数据到明细表
insert into ods_weblog_detail partition(datestr='2012-01-04')
select remote_addr,session,time_local,substring(time_local,0,10) as daystr,substring(time_local,12) as timestr,substring(time_local,0,4) as yearstr,substring(time_local,6,2) as month,
substring(time_local,9,2) as day,substring(time_local,12,2) as hour,split(request,"\\?")[0],status
,body_bytes_sent,http_referer,parse_url(http_referer,'HOST') as ref_host,parse_url(http_referer,'PATH') as ref_path,
parse_url(http_referer,'QUERY') as ref_query,getBrowser(http_user_agent) as http_user_agent, 
getOS(http_user_agent) as os ,getProvince(remote_addr),getCity(remote_addr) from ods_click_pageviews;



#4.模块开发 ----统计分析

#以时间维度统计
select count(1),yearstr,month,day,hour from ods_weblog_detail
group by yearstr,month,day,hour;

##每一个小时来统计PV
drop table dw_pvs_hour;
create table dw_pvs_hour(year string,month string,day string,hour string,pvs bigint) 
row format delimited
fields terminated by '\t';

###插入数据
insert into table dw_pvs_hour
select a.yearstr as year ,a.month as month,a.day as day,a.hour as hour,
count(1) as pvs 
from ods_weblog_detail a
group by a.yearstr,a.month,a.day,a.hour;

##以天为维度来进行统计PV
drop table dw_pvs_day;
create table dw_pvs_day(pvs bigint,year string,month string,day string)
row format delimited
fields terminated by '\t';
###插入数据
insert into table dw_pvs_day
select count(1) as pvs,a.year as year,a.month as month,a.day as day  from dim_time a
join ods_weblog_detail b 
on  a.year=b.yearstr and a.month=b.month and a.day=b.day
group by a.year,a.month,a.day;

##以浏览器类型来进行统计
drop table dw_pvs_browser;
create table dw_pvs_browser(pvs bigint,browser string,
year string,month string,day string)
row format delimited
fields terminated by '\t';
###导入数据
insert into dw_pvs_browser
select count(1) as pvs, a.browser as browser,
b.yearstr as year,
b.month as month,b.day 
as day from dim_browser a
join ods_weblog_detail b
on a.browser=b.http_user_agent 
group by a.browser,b.yearstr,month,day order by pvs desc;

##按照操作系统来进行统计
drop table dw_pvs_os;
create table dw_pvs_os(
pvs bigint,
os string,
year string,
month string,
day string
);

insert into dw_pvs_os
select count(1) as pvs, a.os as os,
b.yearstr as year,
b.month as month,b.day 
as day from dim_os a
join ods_weblog_detail b
on a.os=b.os 
group by a.os,b.yearstr,month,day order by pvs desc;

##按照地域的维度去统计PV
drop table dw_pvs_region;
create table dw_pvs_region(pvs bigint,province string,
city string,year string,
month string,day string)
row format delimited
fields terminated by '\t';
###导入数据
insert into dw_pvs_region
select count(1) as pvs,a.province as province,
a.city as city,b.yearstr as year,
b.month as month,b.day as day from dim_region a
join ods_weblog_detail b on 
a.province=b.province and a.city=b.city 
group by a.province,a.city,b.yearstr,month,day order by pvs desc;

##统计uv
drop table dw_uv;
create table dw_uv(
uv int,
year varchar(4),
month varchar(2),
day varchar(2)
);
###导入数据
insert into dw_uv
select count(1) as uv,a.yearstr as year,
a.month as month,a.day as day from
(select distinct remote_user,yearstr,month,day from ods_weblog_detail) a
group by a.yearstr,a.month,a.day;

##统计IP 
drop table dw_ip;
create table dw_ip(
ip int,
year varchar(4),
month varchar(2),
day varchar(2)
);
###导入数据
insert into dw_ip
select count(1) as ip,a.yearstr as year,
a.month as month,a.day as day from
(select distinct remote_addr,yearstr,month,day from ods_weblog_detail) a
group by a.yearstr,a.month,a.day;

#人均浏览页面
##总的请求页面／去重的人数
drop table dw_avgpv_user_d;
create table dw_avgpv_user_d(
day string,
avgpv string);
###插入数据
insert into table dw_avgpv_user_d
select '2012-01-14',sum(b.pvs)/count(b.remote_user) from
(select remote_user,count(1) as pvs from ods_weblog_detail where datestr='2012-01-04' group by remote_user) b;


#按referer维度统计pv总量
##按照小时为进行统计
drop table dw_pvs_referer_h;
create table dw_pvs_referer_h(referer_url string,referer_host string,year string,month string,day string,hour string,pv_referer_cnt bigint);
###插入数据
insert into table dw_pvs_referer_h
select split(http_referer,"\\?")[0],ref_host,yearstr,month,day,hour,count(1) as pv_referer_cnt
from ods_weblog_detail 
group by http_referer,ref_host,yearstr,month,day,hour 
having ref_host is not null
order by hour asc,day asc,month asc,yearstr asc,pv_referer_cnt desc;

#将需要展示的数据导入到mysql
##mysql 需要建立的表
drop table dw_pvs_hour;
create table dw_pvs_hour(
id int primary key auto_increment,
year varchar(4),
month varchar(2),day varchar(2),
hour varchar(2),pvs int);

###sqoop导入数据
bin/sqoop export --connect jdbc:mysql://10.211.55.16:3306/log --username root --password root --table dw_pvs_day --columns pvs,year,month,day --export-dir '/user/hive/warehouse/loganalyze.db/dw_pvs_day/' --fields-terminated-by '\t';
drop table dw_pvs_day;
create table dw_pvs_day(
id int primary key auto_increment,
year varchar(4),
month varchar(2),
day varchar(2),
pvs int);

###sqoop导入数据
bin/sqoop export --connect jdbc:mysql://10.211.55.16:3306/log --username root --password root --table dw_pvs_browser --columns pvs,browser,year,month,day --export-dir '/user/hive/warehouse/loganalyze.db/dw_pvs_browser/' --fields-terminated-by '\t';
drop table dw_pvs_browser;
create table dw_pvs_browser(
id int primary key auto_increment,
browser varchar(20),
year varchar(4),
month varchar(2),
day varchar(2),
pvs int);

create table dw_pvs_os(
id int primary key auto_increment,
pvs bigint,
os varchar(10),
year varchar(4),
month varchar(2),
day varchar(2)
);

###sqoop导入数据
bin/sqoop export --connect jdbc:mysql://10.211.55.16:3306/log --username root --password root --table dw_pvs_region --columns pvs,province,city,year,month,day --export-dir '/user/hive/warehouse/loganalyze.db/dw_pvs_region/' --fields-terminated-by '\t';
drop table dw_pvs_region;
create table dw_pvs_region(
id int primary key auto_increment,
province varchar(20),
city varchar(20),
year varchar(4),
month varchar(2),
day varchar(2),
pvs int);

###统计uv
drop table dw_uv;
create table dw_uv(
id int primary key auto_increment,
year varchar(4),
month varchar(2),
day varchar(2),
uv int);

##统计ip
drop table dw_ip;
create table dw_ip(
id int primary key auto_increment,
year varchar(4),
month varchar(2),
day varchar(2),
ip int);

##统计人均访问页面
drop table dw_avgpv_user_d;
create table dw_avgpv_user_d(
id int primary key auto_increment,
day varchar(12),
avgpv float);

drop table dw_pvs_referer_h;
create table dw_pvs_referer_h(
id int primary key auto_increment,
referer_url varchar(800),
referer_host varchar(200),
year varchar(4),
month varchar(2),
day varchar(2),
hour varchar(2),
pv_referer_cnt bigint);
###插入数据
#6.数据展示
#使用的技术是Jquery + Echarts + springmvc + spring + mybatis + mysql



