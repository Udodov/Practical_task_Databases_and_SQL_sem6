-- используем БД, созданную в lesson_4 из скрипта "ddl_insert.sql"
USE lesson_4;


/* 
1. Создайте таблицу users_old, аналогичную таблице users. 
Создайте процедуру, с помощью которой можно переместить любого (одного) 
пользователя из таблицы users в таблицу users_old. 
(использование транзакции с выбором commit или rollback – обязательно).
*/
-- пользователи users_old
DROP TABLE IF EXISTS users_old;
CREATE TABLE users_old (
  id SERIAL PRIMARY KEY,
  firstname VARCHAR(50),
  lastname VARCHAR(50),
  email VARCHAR(120) UNIQUE
);

INSERT INTO users_old (id, firstname, lastname, email) 
  VALUES 
  (1, 'Иван', 'Иванов', 'i.ivanov@example.org'),
  (2, 'Петр', 'Петров', 'p.petrov@example.org'),
  (3, 'Сидор', 'Сидоров', 's.sidorov@example.org');

SELECT * FROM users_old;
SELECT * FROM users;


/*
создание процедуры для перемещения любого (одного) пользователя из таблицы users 
в таблицу users_old c определением COMMIT или ROLLBACK 
*/

DROP PROCEDURE IF EXISTS sp_move_user_to_old_table;
DELIMITER //
CREATE PROCEDURE sp_move_user_to_old_table(IN user_id INT)
BEGIN

/* Первым шагом, объявляем обработчик ошибок, который при возникновении ошибки отменяет
все изменения в базе данных и возвращает ее в исходное состояние.*/
  DECLARE EXIT HANDLER FOR SQLEXCEPTION ROLLBACK;
  START TRANSACTION;
  
  -- Получаем данные пользователя из таблицы users
  SELECT firstname, lastname, email INTO @firstname, @lastname, @email
    FROM users 
  WHERE id = user_id;
  
  -- Удаляем пользователя из таблицы users
  DELETE FROM users 
  WHERE id = user_id;
  
  -- Добавляем пользователя в таблицу users_old
  INSERT INTO users_old (firstname, lastname, email) VALUES (@firstname, @lastname, @email);
  
  COMMIT;
END//
DELIMITER ; 

-- Вызов процедуры для перемещения пользователя с id = 3 в таблицу users_old
CALL sp_move_user_to_old_table(3);


-- или 
DROP PROCEDURE IF EXISTS sp_move_user_to_old_table2;
DELIMITER //
CREATE PROCEDURE sp_move_user_to_old_table2(IN user_id INT)
BEGIN  

  DECLARE `_rollback` BIT DEFAULT b'0';
  DECLARE code varchar(100);
  DECLARE error_string varchar(100); 

  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
    BEGIN
	  SET `_rollback` = b'1';
	  GET stacked DIAGNOSTICS CONDITION 1
	    code = RETURNED_SQLSTATE, error_string = MESSAGE_TEXT;
	END;
    
  START TRANSACTION;
  
  -- Получаем данные пользователя из таблицы users
  SELECT firstname, lastname, email INTO @firstname, @lastname, @email
    FROM users 
  WHERE id = user_id;
  
  -- Удаляем пользователя из таблицы users
  DELETE FROM users 
  WHERE id = user_id;
  
  -- Добавляем пользователя в таблицу users_old
  INSERT INTO users_old (firstname, lastname, email) VALUES (@firstname, @lastname, @email);

    IF `_rollback` THEN
		SET user_id = CONCAT('ой. Ошибка: ', code, ' Текст ошибки: ', error_string);
		ROLLBACK;
	ELSE
		SET user_id = 'OK';
		COMMIT;
	END IF;
END//
DELIMITER ;

-- Вызов процедуры для перемещения пользователя с id = 2 в таблицу users_old
CALL sp_move_user_to_old_table2(2);


-- Создаем отдельную БД
DROP DATABASE IF EXISTS pt_dasql_sem6; 
CREATE DATABASE IF NOT EXISTS pt_dasql_sem6;
USE pt_dasql_sem6;

/* 
Создайте хранимую функцию hello(), которая будет возвращать приветствие, в зависимости от текущего времени суток. 
С 6:00 до 12:00 функция должна возвращать фразу "Доброе утро", 
с 12:00 до 18:00 функция должна возвращать фразу "Добрый день", 
с 18:00 до 00:00 — "Добрый вечер", 
с 00:00 до 6:00 — "Доброй ночи".
*/

DROP FUNCTION IF EXISTS hello;
DELIMITER //
CREATE FUNCTION hello()
RETURNS VARCHAR(50)
DETERMINISTIC
BEGIN
  DECLARE `current_time` TIME;
  DECLARE greeting VARCHAR(50);
    
  SET `current_time` = CURRENT_TIME();
    
  IF `current_time` >= '06:00:00' AND `current_time` < '12:00:00' THEN
    SET greeting = 'Доброе утро';
  ELSEIF `current_time` >= '12:00:00' AND `current_time` < '18:00:00' THEN
    SET greeting = 'Добрый день';
  ELSEIF `current_time` >= '18:00:00' OR `current_time` < '00:00:00' THEN
    SET greeting = 'Добрый вечер';
  ELSEIF `current_time` >= '00:00:00' OR `current_time` < '06:00:00' THEN
    SET greeting = 'Доброй ночи';
  END IF;
    
  RETURN greeting;
END//
DELIMITER ;

SELECT hello();


/*
3. (по желанию)* Создайте таблицу logs типа Archive. 
Пусть при каждом создании записи в таблицах users, communities и messages в таблицу logs 
помещается время и дата создания записи, название таблицы, идентификатор первичного ключа.
*/

-- используем БД, созданную в lesson_4 из скрипта "ddl_insert.sql"
USE lesson_4;

DROP TABLE IF EXISTS `logs`;

CREATE TABLE `logs` (
id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
created_at DATETIME DEFAULT NOW(),
`table_name` VARCHAR(50),
primary_key_id BIGINT UNSIGNED
) ENGINE=ARCHIVE;

CREATE TRIGGER users_insert_trigger AFTER INSERT ON users
FOR EACH ROW
INSERT INTO `logs` (`table_name`, primary_key_id) VALUES ('users', NEW.id);

CREATE TRIGGER communities_insert_trigger AFTER INSERT ON communities
FOR EACH ROW
INSERT INTO `logs` (`table_name`, primary_key_id) VALUES ('communities', NEW.id);

CREATE TRIGGER messages_insert_trigger AFTER INSERT ON messages
FOR EACH ROW
INSERT INTO `logs` (`table_name`, primary_key_id) VALUES ('messages', NEW.id);

SELECT * FROM `logs`;
SELECT * FROM users;
SELECT * FROM communities;
SELECT * FROM messages;

INSERT INTO users (id, firstname, lastname, email) 
  VALUES 
  (11, 'Константин', 'Удодов', 'k.udodov@bk.ru');
  
INSERT INTO `communities` (`name`) 
  VALUES 
  ('Educational platform "GeekBrains"');
  
INSERT INTO messages (from_user_id, to_user_id, body, created_at) 
  VALUES
  (11, 10, 'Thanks to all the teachers of the GeekBrains platform, for the introduction of the course "Databases and SQL", for informative lectures and effective seminars!',  DATE_ADD(NOW(), INTERVAL 1 MINUTE));