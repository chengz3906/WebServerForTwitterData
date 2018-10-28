-- using mysql 5.7.11+
-- before logging into your database ...
-- mysql --username=username --password=user_password --local-infile=1
-- or if remotely to something like if you are using RDS
-- mysql --username=username -h db_hostname --port db_port --password=user_password --local-infile=1

-- Step 1 create database
drop database if exists twitter_db;
create database twitter_db;
use twitter_db;

-- Step 2 create businesses table
drop table if exists `contact_user`;
create table `contact_user` (
	`id` bigint(20) not null,
	`description` text default null,
	`screen_name` LONGTEXT default null,
	primary key (id),
	character
);

-- Step 3 load data to businesses table
load data local infile 'user-part-00000.csv' into table contact_user columns
terminated by '\t' LINES TERMINATED BY '\n';

-- Step 4 create checkins table
drop table if exists `contact_tweet`;
create table `contact_tweet` (
	`user1_id` bigint(20) not null,
	`user2_id` bigint(20) not null,
	`tweet_text` text default null,
	`created_at` timestamp not null,
	`intimacy_score` double not null,
	foreign key (user1_id) references contact_user(id),
	foreign key (user2_id) references contact_user(id)
);

-- Step 5 load data to checkins table
-- load data local infile 'yelp_academic_dataset_checkin.tsv' into table checkins columns terminated by '\t' LINES TERMINATED BY '\r\n' IGNORE 1 LINES;
