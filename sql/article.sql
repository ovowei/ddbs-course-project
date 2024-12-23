DROP TABLE IF EXISTS `article_temp`;
CREATE TABLE `article_temp` (
  `timestamp` char(14) DEFAULT NULL,
  `id` char(7) DEFAULT NULL,
  `aid` char(7) DEFAULT NULL,
  `title` char(15) DEFAULT NULL,
  `category` char(11) DEFAULT NULL,
  `abstract` char(30) DEFAULT NULL,
  `articleTags` char(14) DEFAULT NULL,
  `authors` char(13) DEFAULT NULL,
  `language` char(3) DEFAULT NULL,
  `text` char(31) DEFAULT NULL,
  `image` char(64) DEFAULT NULL,
  `video` char(32) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOAD DATA INFILE '/mnt/djw/db_generator/article.dat'
INTO TABLE `article_temp`
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n';
