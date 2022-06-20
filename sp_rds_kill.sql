/*
CREATE TEMPORARY TABLE information_schema.PROCESSLIST (
  ID bigint(21) unsigned NOT NULL DEFAULT '0',
  USER varchar(16) NOT NULL DEFAULT '',
  HOST varchar(64) NOT NULL DEFAULT '',
  DB varchar(64) DEFAULT NULL,
  COMMAND varchar(16) NOT NULL DEFAULT '',
  TIME int(7) NOT NULL DEFAULT '0',
  STATE varchar(64) DEFAULT NULL,
  INFO longtext
) ENGINE=MyISAM DEFAULT CHARSET=utf8

CREATE TEMPORARY TABLE information_schema.INNODB_LOCK_WAITS (
  requesting_trx_id varchar(18) NOT NULL DEFAULT '',
  requested_lock_id varchar(81) NOT NULL DEFAULT '',
  blocking_trx_id varchar(18) NOT NULL DEFAULT '',
  blocking_lock_id varchar(81) NOT NULL DEFAULT ''
) ENGINE=MEMORY DEFAULT CHARSET=utf8
;

CREATE TEMPORARY TABLE information_schema.INNODB_TRX (
  trx_id varchar(18) NOT NULL DEFAULT '',
  trx_state varchar(13) NOT NULL DEFAULT '',
  trx_started datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  trx_requested_lock_id varchar(81) DEFAULT NULL,
  trx_wait_started datetime DEFAULT NULL,
  trx_weight bigint(21) unsigned NOT NULL DEFAULT '0',
  trx_mysql_thread_id bigint(21) unsigned NOT NULL DEFAULT '0',
  trx_query varchar(1024) DEFAULT NULL,
  trx_operation_state varchar(64) DEFAULT NULL,
  trx_tables_in_use bigint(21) unsigned NOT NULL DEFAULT '0',
  trx_tables_locked bigint(21) unsigned NOT NULL DEFAULT '0',
  trx_lock_structs bigint(21) unsigned NOT NULL DEFAULT '0',
  trx_lock_memory_bytes bigint(21) unsigned NOT NULL DEFAULT '0',
  trx_rows_locked bigint(21) unsigned NOT NULL DEFAULT '0',
  trx_rows_modified bigint(21) unsigned NOT NULL DEFAULT '0',
  trx_concurrency_tickets bigint(21) unsigned NOT NULL DEFAULT '0',
  trx_isolation_level varchar(16) NOT NULL DEFAULT '',
  trx_unique_checks int(1) NOT NULL DEFAULT '0',
  trx_foreign_key_checks int(1) NOT NULL DEFAULT '0',
  trx_last_foreign_key_error varchar(256) DEFAULT NULL,
  trx_adaptive_hash_latched int(1) NOT NULL DEFAULT '0',
  trx_adaptive_hash_timeout bigint(21) unsigned NOT NULL DEFAULT '0',
  trx_is_read_only int(1) NOT NULL DEFAULT '0',
  trx_autocommit_non_locking int(1) NOT NULL DEFAULT '0'
) ENGINE=MEMORY DEFAULT CHARSET=utf8
;

CREATE TABLE performance_schema.threads (
  THREAD_ID bigint(20) unsigned NOT NULL,
  NAME varchar(128) NOT NULL,
  TYPE varchar(10) NOT NULL,
  PROCESSLIST_ID bigint(20) unsigned DEFAULT NULL,
  PROCESSLIST_USER varchar(32) DEFAULT NULL,
  PROCESSLIST_HOST varchar(256) DEFAULT NULL,
  PROCESSLIST_DB varchar(64) DEFAULT NULL,
  PROCESSLIST_COMMAND varchar(16) DEFAULT NULL,
  PROCESSLIST_TIME bigint(20) DEFAULT NULL,
  PROCESSLIST_STATE varchar(64) DEFAULT NULL,
  PROCESSLIST_INFO longtext,
  PARENT_THREAD_ID bigint(20) unsigned DEFAULT NULL,
  ROLE varchar(64) DEFAULT NULL,
  INSTRUMENTED enum('YES','NO') NOT NULL,
  HISTORY enum('YES','NO') NOT NULL,
  CONNECTION_TYPE varchar(16) DEFAULT NULL,
  THREAD_OS_ID bigint(20) unsigned DEFAULT NULL
) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8
;

CREATE TABLE performance_schema.events_statements_current (
  THREAD_ID bigint(20) unsigned NOT NULL,
  EVENT_ID bigint(20) unsigned NOT NULL,
  END_EVENT_ID bigint(20) unsigned DEFAULT NULL,
  EVENT_NAME varchar(128) NOT NULL,
  SOURCE varchar(64) DEFAULT NULL,
  TIMER_START bigint(20) unsigned DEFAULT NULL,
  TIMER_END bigint(20) unsigned DEFAULT NULL,
  TIMER_WAIT bigint(20) unsigned DEFAULT NULL,
  LOCK_TIME bigint(20) unsigned NOT NULL,
  SQL_TEXT longtext,
  DIGEST varchar(32) DEFAULT NULL,
  DIGEST_TEXT longtext,
  CURRENT_SCHEMA varchar(64) DEFAULT NULL,
  OBJECT_TYPE varchar(64) DEFAULT NULL,
  OBJECT_SCHEMA varchar(64) DEFAULT NULL,
  OBJECT_NAME varchar(64) DEFAULT NULL,
  OBJECT_INSTANCE_BEGIN bigint(20) unsigned DEFAULT NULL,
  MYSQL_ERRNO int(11) DEFAULT NULL,
  RETURNED_SQLSTATE varchar(5) DEFAULT NULL,
  MESSAGE_TEXT varchar(128) DEFAULT NULL,
  ERRORS bigint(20) unsigned NOT NULL,
  WARNINGS bigint(20) unsigned NOT NULL,
  ROWS_AFFECTED bigint(20) unsigned NOT NULL,
  ROWS_SENT bigint(20) unsigned NOT NULL,
  ROWS_EXAMINED bigint(20) unsigned NOT NULL,
  CREATED_TMP_DISK_TABLES bigint(20) unsigned NOT NULL,
  CREATED_TMP_TABLES bigint(20) unsigned NOT NULL,
  SELECT_FULL_JOIN bigint(20) unsigned NOT NULL,
  SELECT_FULL_RANGE_JOIN bigint(20) unsigned NOT NULL,
  SELECT_RANGE bigint(20) unsigned NOT NULL,
  SELECT_RANGE_CHECK bigint(20) unsigned NOT NULL,
  SELECT_SCAN bigint(20) unsigned NOT NULL,
  SORT_MERGE_PASSES bigint(20) unsigned NOT NULL,
  SORT_RANGE bigint(20) unsigned NOT NULL,
  SORT_ROWS bigint(20) unsigned NOT NULL,
  SORT_SCAN bigint(20) unsigned NOT NULL,
  NO_INDEX_USED bigint(20) unsigned NOT NULL,
  NO_GOOD_INDEX_USED bigint(20) unsigned NOT NULL,
  NESTING_EVENT_ID bigint(20) unsigned DEFAULT NULL,
  NESTING_EVENT_TYPE enum('TRANSACTION','STATEMENT','STAGE','WAIT') DEFAULT NULL,
  NESTING_EVENT_LEVEL int(11) DEFAULT NULL
) ENGINE=PERFORMANCE_SCHEMA DEFAULT CHARSET=utf8
;
*/

/*
https://aws.amazon.com/ko/premiumsupport/knowledge-center/blocked-mysql-query/
block 트랜잭션 확인.
*/
SELECT
tr.THREAD_ID
FROM information_schema.INNODB_LOCK_WAITS AS w
INNER JOIN information_schema.INNODB_TRX AS tx
ON w.blocking_trx_id = tx.trx_id
INNER JOIN performance_schema.threads AS tr
ON tr.PROCESSLIST_ID = tx.trx_mysql_thread_id
WHERE tr.PROCESSLIST_USER='_soda'
AND tr.PROCESSLIST_DB='soda'
AND tr.PROCESSLIST_INFO LIKE 'SELECT%'
;

DROP PROCEDURE IF EXISTS mysql.sp_rds_kill_thread;
delimiter //
CREATE PROCEDURE mysql.sp_rds_kill_thread()
BEGIN
	-- Save data read from the cursor created from mysql.tables_priv.
	DECLARE v_id bigint;
	-- Whether the cursor has been read to the end
    DECLARE v_no_more BOOLEAN DEFAULT FALSE;
    -- Create cursor.
    DECLARE v_cursor CURSOR FOR
	SELECT
	p.ID
	FROM information_schema.PROCESSLIST AS p
	WHERE p.USER='_soda'
    AND p.DB='soda'
    AND p.INFO LIKE '%main_channeloperator%'
    ;
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_no_more := TRUE;

	OPEN v_cursor;
	-- Loop begin
	LOOP_FIRST: LOOP
		FETCH v_cursor INTO v_id;
		-- Cursor close and loop end after all records have been read.
		IF v_no_more THEN
			CLOSE v_cursor;
			LEAVE LOOP_FIRST;
		END IF;

        SET @s = CONCAT('CALL mysql.rds_kill(',v_id,');');
        SELECT @s;
        -- PREPARE stmt FROM @s;
        -- EXECUTE stmt;
	END LOOP LOOP_FIRST;
END //
delimiter ;
/* --------
End of sp_rds_kill_thread Procedure
 --------*/

DROP PROCEDURE IF EXISTS mysql.sp_blocking_trx_kill;
delimiter //
CREATE PROCEDURE mysql.sp_blocking_trx_kill()
BEGIN
	-- Save data read from the cursor created from mysql.tables_priv.
	DECLARE v_id bigint;
	-- Whether the cursor has been read to the end
    DECLARE v_no_more BOOLEAN DEFAULT FALSE;
    -- Create cursor.
    DECLARE v_cursor CURSOR FOR
    SELECT
    tr.THREAD_ID
    FROM information_schema.INNODB_LOCK_WAITS AS w
    INNER JOIN information_schema.INNODB_TRX AS tx
    ON w.blocking_trx_id = tx.trx_id
    INNER JOIN performance_schema.threads AS tr
    ON tr.PROCESSLIST_ID = tx.trx_mysql_thread_id
    ;
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_no_more := TRUE;
	DECLARE CONTINUE HANDLER FOR 1094 SET @e='Error, Unknown thread';

	OPEN v_cursor;
	-- Loop begin
	LOOP_FIRST: LOOP
		FETCH v_cursor INTO v_id;
		-- Cursor close and loop end after all records have been read.
		IF v_no_more THEN
			CLOSE v_cursor;
			LEAVE LOOP_FIRST;
		END IF;

        SET @s = CONCAT('CALL mysql.rds_kill(',v_id,');');
        -- SELECT @s;
        PREPARE stmt FROM @s;
        EXECUTE stmt;
	END LOOP LOOP_FIRST;
END //
delimiter ;

DROP EVENT IF EXISTS mysql.ev_blocking_trx_kill;
CREATE EVENT mysql.ev_blocking_trx_kill ON SCHEDULE EVERY 1 SECOND COMMENT 'Blocking trx threads kill.' DO CALL mysql.sp_blocking_trx_kill();

DROP PROCEDURE IF EXISTS mysql.sp_chat_kill_thread;
delimiter //
CREATE PROCEDURE mysql.sp_chat_kill_thread()
BEGIN
	-- Save data read from the cursor created from mysql.tables_priv.
	DECLARE v_id bigint;
	-- Whether the cursor has been read to the end
  DECLARE v_no_more BOOLEAN DEFAULT FALSE;
  -- Create cursor.
  DECLARE v_cursor CURSOR FOR
  SELECT
  tr.THREAD_ID
  FROM performance_schema.threads AS tr
  WHERE tr.PROCESSLIST_USER='_soda'
  AND tr.PROCESSLIST_DB='soda'
  AND tr.PROCESSLIST_INFO LIKE 'SELECT * FROM `main_chatmessage` FORCE INDEX (`main_chatmessage_ts_214265cdedd4296c_idx`) WHERE `application_id`=3 AND `cmd` IN (\'MESG\',\'FILE\',\'BRDM\') AND `channel_id` IN%'
  AND tr.PROCESSLIST_TIME > 0
  ;
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_no_more := TRUE;
  DECLARE CONTINUE HANDLER FOR 1094 SET @e='Error, Unknown thread';

	OPEN v_cursor;
	-- Loop begin
	LOOP_FIRST: LOOP
		FETCH v_cursor INTO v_id;
		-- Cursor close and loop end after all records have been read.
		IF v_no_more THEN
			CLOSE v_cursor;
			LEAVE LOOP_FIRST;
		END IF;

        SET @s = CONCAT('CALL mysql.rds_kill(',v_id,');');
        -- SELECT @s;
        PREPARE stmt FROM @s;
        EXECUTE stmt;
	END LOOP LOOP_FIRST;
END //
delimiter ;

DROP EVENT IF EXISTS mysql.ev_chat_kill_thread;
CREATE EVENT mysql.ev_chat_kill_thread ON SCHEDULE EVERY 1 SECOND COMMENT 'select start main_chatmessage trx threads kill.' DO CALL mysql.sp_chat_kill_thread();

SELECT *
FROM information_schema.EVENTS
;