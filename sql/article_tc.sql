DROP TABLE IF EXISTS `article`;
CREATE TABLE `article` (
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
  `image` char(32) DEFAULT NULL,
  `video` char(32) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOCK TABLES `article_temp` READ, `user` WRITE;

INSERT INTO `article`
SELECT *
FROM `article_temp`
WHERE `category` = 'technology';

UNLOCK TABLES;

DROP TABLE IF EXISTS `article_temp`; 