DROP TABLE IF EXISTS `user_temp`;

CREATE TABLE `user_temp` (
  `timestamp` char(14) DEFAULT NULL,
  `id` char(5) DEFAULT NULL,
  `uid` char(5) DEFAULT NULL,
  `name` char(9) DEFAULT NULL,
  `gender` char(7) DEFAULT NULL,
  `email` char(10) DEFAULT NULL,
  `phone` char(10) DEFAULT NULL,
  `dept` char(9) DEFAULT NULL,
  `grade` char(7) DEFAULT NULL,
  `language` char(3) DEFAULT NULL,
  `region` char(10) DEFAULT NULL,
  `role` char(6) DEFAULT NULL,
  `preferTags` char(7) DEFAULT NULL,
  `obtainedCredits` char(3) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOAD DATA INFILE '/mnt/djw/db_generator/user.dat'
INTO TABLE `user_temp`
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n';

