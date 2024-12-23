DROP TABLE IF EXISTS `user_read`;
CREATE TABLE `user_read` (
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

LOCK TABLES `user_read_temp` READ, `user` READ, `user_read` WRITE;


INSERT INTO `user_read`
SELECT `user_read_temp`.*
FROM `user_read_temp`
JOIN `user`
ON `user_read_temp`.uid = `user`.uid
WHERE `user`.region = 'Hong Kong';


UNLOCK TABLES;

DROP TABLE IF EXISTS `user_read_temp`; 