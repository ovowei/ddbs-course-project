DROP TABLE IF EXISTS `user_read_temp`;
CREATE TABLE `user_read_temp` (
  `timestamp` char(14) DEFAULT NULL,
  `id` char(7) DEFAULT NULL,
  `uid` char(5) DEFAULT NULL,
  `aid` char(7) DEFAULT NULL,
  `readTimeLength` char(3) DEFAULT NULL,
  `agreeOrNot` char(2) DEFAULT NULL,
  `commentOrNot` char(2) DEFAULT NULL,
  `shareOrNot` char(2) DEFAULT NULL,
  `commentDetail` char(100) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOAD DATA INFILE '/mnt/djw/db_generator/read.dat'
INTO TABLE `user_read_temp`
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n';
