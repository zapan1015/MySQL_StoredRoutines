-- Event Scheduler:
SHOW variables LIKE '%event_scheduler%';
SHOW variables LIKE '%max_connections%';
-- 3000

-- Events drop
SELECT concat('DROP EVENT IF EXISTS ',db,'.',name,';')
FROM mysql.event
;
SELECT NOW(),
event.*
FROM mysql.event
;
SELECT *
FROM information_schema.EVENTS 
;

-- DB Drop
DROP DATABASE IF EXISTS sbdesk;
DROP DATABASE IF EXISTS tmp;
DROP DATABASE IF EXISTS slow_logs;

--
EXPLAIN EXTENDED;

-- Table dop
SELECT
concat('TRUNCATE TABLE soda.',T.TABLE_NAME,';DROP TABLE IF EXISTS soda.',T.TABLE_NAME,';')
FROM information_schema.TABLES AS T
WHERE T.TABLE_SCHEMA = 'soda'
AND T.TABLE_TYPE = 'BASE TABLE'
AND T.TABLE_NAME NOT IN (
'main_application',
'main_heavyuser',
'main_messagingchannellist',
'main_channel',
'main_chatmessage',
'main_channel_members',
'main_channeldata',
'main_profile'
)
AND T.TABLE_NAME NOT LIKE 'main_user%'
;

SELECT
concat('ANALYZE TABLE soda.',T.TABLE_NAME,';')
FROM information_schema.TABLES AS T
WHERE T.TABLE_SCHEMA = 'soda'
AND T.TABLE_TYPE = 'BASE TABLE'
;

-- Temp User channel count
-- https://dev.mysql.com/doc/refman/5.7/en/create-database.html
DROP DATABASE IF EXISTS sb_temp;
CREATE DATABASE IF NOT EXISTS sb_temp CHARACTER SET=utf8;

-- User의 channel count
DROP TABLE IF EXISTS sb_temp.temp_rows_count;
CREATE TABLE IF NOT EXISTS sb_temp.temp_rows_count (
    cnt_type varchar(20) NOT NULL,
    tot_cnt bigint unsigned NOT NULL,
    PRIMARY KEY (cnt_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
;

TRUNCATE TABLE sb_temp.temp_rows_count;

CREATE EVENT sb_temp.event_temp_rows_count
ON SCHEDULE AT CURRENT_TIMESTAMP DO
INSERT INTO sb_temp.temp_rows_count (cnt_type, tot_cnt)
SELECT 'mcl rows' AS cnt_type, COUNT(*) AS tot_cnt
FROM soda.main_messagingchannellist
;

SELECT
trc.cnt_type,
trc.tot_cnt
FROM sb_temp.temp_rows_count AS trc;

-- User의 channel count
-- tmp_table_size = 134217728 (128MB), 33554432 (32MB)
-- sort_buffer_size = 1048576 (1MB)
DROP TABLE IF EXISTS sb_temp.temp_user_mcl_stat;
CREATE TABLE IF NOT EXISTS sb_temp.temp_user_mcl_stat (
    user_id bigint unsigned NOT NULL,
    min_mcl_id bigint unsigned NOT NULL,
    max_mcl_id bigint unsigned NOT NULL,
    channel_count bigint unsigned NOT NULL,
    PRIMARY KEY (user_id),
    KEY ix_temp_user_mcl_stat_01 (channel_count)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
;

CREATE EVENT sb_temp.event_temp_user_mcl_stat
ON SCHEDULE AT CURRENT_TIMESTAMP DO
INSERT INTO sb_temp.temp_user_mcl_stat (user_id, min_mcl_id, max_mcl_id, channel_count)
SELECT user_id, MIN(id) AS min_mcl_id, MAX(id) AS max_mcl_id, COUNT(*) AS channel_count
FROM soda.main_messagingchannellist
GROUP BY
user_id
;

SELECT tu.user_id,
    tu.min_mcl_id,
    tu.max_mcl_id,
    tu.channel_count
FROM sb_temp.temp_user_mcl_stat AS tu
;

INSERT INTO sb_temp.temp_user_channel_count (user_id, channel_count)
VALUES (1,2,3)
ON DUPLICATE KEY UPDATE channel_count=VALUES(channel_count);

DROP PROCEDURE IF EXISTS sb_temp.sp_temp_user_channel_count;
delimiter //
CREATE PROCEDURE sb_temp.sp_temp_user_channel_count(IN v_share INT, IN v_num INT)
BEGIN
    -- Max profile id
    SELECT id INTO @max_id FROM soda.main_profile ORDER BY id DESC limit 1;
    SET @v_chunk_size = CEIL( @max_id / v_share );
    SET @start_loop = ((v_num - 1) * @v_chunk_size) + 1;
    SET @end_loop = v_num * @v_chunk_size;
    SET @loop_chunk = 1000;
    SET @start_id = @start_loop;
    
    REPEAT
        SET @end_id = @start_id + @loop_chunk;
		-- cursor begin block
		BLOCK_SECOND: BEGIN
			DECLARE v_user_id bigint;
			DECLARE v_no_more_priv BOOLEAN DEFAULT FALSE;
			DECLARE v_cursor CURSOR FOR
            SELECT main_profile.user_id
            FROM soda.main_profile
            WHERE main_profile.id BETWEEN @start_id AND @end_id
            ;
			DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_no_more_priv := TRUE;
			OPEN v_cursor;
            -- SELECT @start_id, @end_id;
			-- Loop begin
			LOOP_FIRST: LOOP
				FETCH v_cursor INTO v_user_id;
				IF v_no_more_priv THEN
					CLOSE v_cursor;
					LEAVE LOOP_FIRST;
				END IF;
				SELECT COUNT(*) INTO @channel_count
                FROM soda.main_messagingchannellist
                WHERE user_id = v_user_id;
                INSERT INTO sb_temp.temp_user_channel_count (user_id, channel_count)
                VALUES (v_user_id, @channel_count)
                ON DUPLICATE KEY UPDATE channel_count=VALUES(channel_count);
                -- SELECT @channel_count;
			END LOOP LOOP_FIRST;
		END BLOCK_SECOND;
		-- cursor end block
        SET @start_id = @loop_chunk;
    UNTIL @end_id >= @end_loop END REPEAT;
    -- SELECT @v_chunk_size, @start_loop, @end_loop;
END //
delimiter ;

DROP EVENT IF EXISTS sb_temp.event_temp_user_channel_count_1;
CREATE EVENT sb_temp.event_temp_user_channel_count_1
	ON SCHEDULE AT CURRENT_TIMESTAMP + INTERVAL 1 MINUTE DO CALL sb_temp.sp_temp_user_channel_count(100, 1);

SELECT temp_user_channel_count.user_id,
    temp_user_channel_count.channel_count
FROM sb_temp.temp_user_channel_count;

-- User channel 임시 테이블.
DROP TABLE IF EXISTS sb_temp.temp_user_channel;
CREATE TABLE IF NOT EXISTS sb_temp.temp_user_channel (
    user_id bigint unsigned NOT NULL,
    channel_id bigint unsigned NOT NULL,
    PRIMARY KEY (user_id, channel_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
;

DROP TABLE IF EXISTS sb_temp.temp_channel_user;
CREATE TABLE IF NOT EXISTS sb_temp.temp_channel_user (
    channel_id bigint unsigned NOT NULL,
    user_id bigint unsigned NOT NULL,
    PRIMARY KEY (channel_id, user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
;

INSERT INTO sb_temp.temp_user_channel_count (user_id, channel_count)
VALUES (1,2,3)
ON DUPLICATE KEY UPDATE channel_count=VALUES(channel_count);

DROP PROCEDURE IF EXISTS sb_temp.sp_temp_user_channel;
delimiter //
CREATE PROCEDURE sb_temp.sp_temp_user_channel(IN v_share INT, IN v_num INT)
BEGIN
    -- Max profile id
    SELECT id INTO @max_id FROM soda.main_messagingchannellist ORDER BY id DESC limit 1;
    SET @v_chunk_size = CEIL( @max_id / v_share );
    SET @start_loop = ((v_num - 1) * @v_chunk_size) + 1;
    SET @end_loop = v_num * @v_chunk_size;
    SET @loop_chunk = 1000;
    SET @start_id = @start_loop;
    
    REPEAT
        SET @end_id = @start_id + @loop_chunk;
		-- cursor begin block
		BLOCK_SECOND: BEGIN
			DECLARE v_user_id bigint;
            DECLARE v_channel_id bigint;
			DECLARE v_no_more_priv BOOLEAN DEFAULT FALSE;
			DECLARE v_cursor CURSOR FOR
            SELECT main_messagingchannellist.user_id,
            main_messagingchannellist.channel_id
            FROM soda.main_messagingchannellist
            WHERE main_messagingchannellist.id BETWEEN @start_id AND @end_id
            ;
			DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_no_more_priv := TRUE;
			OPEN v_cursor;
            -- SELECT @start_id, @end_id;
			-- Loop begin
			LOOP_FIRST: LOOP
				FETCH v_cursor INTO v_user_id, v_channel_id;
				IF v_no_more_priv THEN
					CLOSE v_cursor;
					LEAVE LOOP_FIRST;
				END IF;
                INSERT INTO sb_temp.temp_user_channel (user_id, channel_id)
                VALUES (v_user_id, v_channel_id);
                INSERT INTO sb_temp.temp_channel_user (user_id, channel_id)
                VALUES (v_user_id, v_channel_id);
                -- SELECT @channel_count;
			END LOOP LOOP_FIRST;
		END BLOCK_SECOND;
		-- cursor end block
        SET @start_id = @loop_chunk;
    UNTIL @end_id >= @end_loop END REPEAT;
    -- SELECT @v_chunk_size, @start_loop, @end_loop;
END //
delimiter ;

DROP EVENT IF EXISTS sb_temp.event_temp_user_channel_1;
CREATE EVENT sb_temp.event_temp_user_channel_1
	ON SCHEDULE AT CURRENT_TIMESTAMP + INTERVAL 1 MINUTE DO CALL sb_temp.sp_temp_user_channel(100, 1);

SELECT temp_channel_user.channel_id,
    temp_channel_user.user_id
FROM sb_temp.temp_channel_user;

SELECT
(SELECT COUNT(*) FROM sb_temp.temp_user_channel) AS user_channel_count,
(SELECT COUNT(*) FROM sb_temp.temp_channel_user) AS channel_user_count
;

-- my group channel list
DROP TABLE IF EXISTS sb_temp.test_unread_message_count_time;
CREATE TABLE sb_temp.test_unread_message_count_time (
	channel_id bigint NOT NULL,
	start_time time(6),
	end_time time(6),
	diff_time time(6),
	PRIMARY KEY (channel_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
;