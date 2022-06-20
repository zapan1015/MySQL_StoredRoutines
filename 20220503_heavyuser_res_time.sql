-- MCL Rows count
SELECT temp_rows_count.cnt_type,
    temp_rows_count.tot_cnt
FROM sb_temp.temp_rows_count;
-- 1982020953

-- MCL User count: 169872643
SELECT
temp_user_mcl_stat.user_id,
temp_user_mcl_stat.min_mcl_id,
temp_user_mcl_stat.max_mcl_id,
temp_user_mcl_stat.channel_count
FROM sb_temp.temp_user_mcl_stat;

-- 18.501 sec
SELECT 
mcl.*,
main_channel.*
FROM soda.main_messagingchannellist AS mcl FORCE INDEX (MAIN_MESSAGINGCHANNELLIST_USER_ID_44927FAD2C548742_IDX)
INNER JOIN soda.main_channel ON (mcl.channel_id = main_channel.id)
WHERE mcl.user_id = 267373095
AND mcl.is_hidden = 0
AND main_channel.removed = 0
AND main_channel.custom_type IN ('official')
AND main_channel.channel_type IN (5, 6, 8, 9, 10, 11, 12, 14, 15, 16, 101)
ORDER BY mcl.last_message_ts DESC, mcl.id DESC
LIMIT 2
;

-- 17.393 sec
SELECT 
mcl.*,
main_channel.*,
main_chatmessage.*
FROM soda.main_messagingchannellist AS mcl
INNER JOIN soda.main_channel ON (mcl.channel_id = main_channel.id)
LEFT OUTER JOIN soda.main_chatmessage ON (mcl.last_message_id = main_chatmessage.id)
WHERE mcl.user_id = 267373095
AND mcl.is_hidden = 0
AND mcl.count_since_joined > 0
AND main_channel.removed = 0
AND main_channel.channel_type IN (5, 6, 8, 9, 10, 11, 12, 14, 15, 16, 101)
ORDER BY
mcl.last_message_ts DESC,
mcl.id DESC
LIMIT 21
;

PREPARE mcl FROM 'SELECT /*!sql_no_cache*/
mcl.*
FROM soda.main_messagingchannellist AS mcl
WHERE mcl.user_id = ?
AND mcl.is_hidden = 0
ORDER BY mcl.last_message_ts DESC , mcl.id DESC
';

PREPARE mcl FROM 'SELECT /*!sql_no_cache*/
mcl.*,
main_channel.*
FROM soda.main_messagingchannellist AS mcl
INNER JOIN soda.main_channel ON (mcl.channel_id = main_channel.id)
WHERE mcl.user_id = ?
AND mcl.is_hidden = 0
AND main_channel.removed = 0
AND main_channel.custom_type IN (''official'')
AND main_channel.channel_type IN (5, 6, 8, 9, 10, 11, 12, 14, 15, 16, 101)
ORDER BY mcl.last_message_ts DESC, mcl.id DESC
LIMIT 2
';

PREPARE mcl FROM 'SELECT /*!sql_no_cache*/
mcl.*,
main_channel.*,
main_chatmessage.*
FROM soda.main_messagingchannellist AS mcl
INNER JOIN soda.main_channel ON (mcl.channel_id = main_channel.id)
LEFT OUTER JOIN soda.main_chatmessage ON (mcl.last_message_id = main_chatmessage.id)
WHERE mcl.user_id = ?
AND mcl.is_hidden = 0
AND mcl.count_since_joined > 0
AND main_channel.removed = 0
AND main_channel.channel_type IN (5, 6, 8, 9, 10, 11, 12, 14, 15, 16, 101)
ORDER BY mcl.last_message_ts DESC, mcl.id DESC
LIMIT 21
';

DROP TABLE IF EXISTS sb_temp.poc_arch_time;
CREATE TABLE sb_temp.poc_arch_time (
user_id bigint NOT NULL,
start_time DATETIME(6),
end_time DATETIME(6),
diff_time time(6),
PRIMARY KEY (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
;

TRUNCATE TABLE sb_temp.poc_arch_time;

DROP PROCEDURE IF EXISTS sb_temp.sp_temp_user_channel_count;
delimiter //
CREATE PROCEDURE sb_temp.sp_temp_user_channel_count(IN v_share INT, IN v_num INT)
BEGIN
    -- Max profile id
    SELECT user_id INTO @max_id FROM sb_temp.temp_user_mcl_stat WHERE channel_count >= 1000 ORDER BY user_id DESC limit 1;
    -- SET @max_id = 1011;
    SET @v_chunk_size = CEIL( @max_id / v_share );
    SET @start_loop = ((v_num - 1) * @v_chunk_size) + 1;
    SET @end_loop = v_num * @v_chunk_size;
    SET @loop_chunk = 1000;
    SET @start_id = @start_loop;
    PREPARE ts1 FROM 'INSERT INTO sb_temp.poc_arch_time (user_id,start_time) VALUES (?,NOW(6))';
    PREPARE ts2 FROM 'UPDATE sb_temp.poc_arch_time SET end_time=NOW(6), diff_time=TIMEDIFF(NOW(6), start_time) WHERE user_id = ?';
PREPARE mcl FROM 'SELECT /*!sql_no_cache*/
mcl.*,
main_channel.*,
main_chatmessage.*
FROM soda.main_messagingchannellist AS mcl
INNER JOIN soda.main_channel ON (mcl.channel_id = main_channel.id)
LEFT OUTER JOIN soda.main_chatmessage ON (mcl.last_message_id = main_chatmessage.id)
WHERE mcl.user_id = ?
AND mcl.is_hidden = 0
AND mcl.count_since_joined > 0
AND main_channel.removed = 0
AND main_channel.channel_type IN (5, 6, 8, 9, 10, 11, 12, 14, 15, 16, 101)
ORDER BY mcl.last_message_ts DESC, mcl.id DESC
LIMIT 21
';

    REPEAT
        IF (@start_id + @loop_chunk) < @end_loop THEN
            SET @end_id = (@start_id - 1) + @loop_chunk;
        ELSE
            SET @end_id = @end_loop;
        END IF;
		-- cursor begin block
		BLOCK_SECOND: BEGIN
			DECLARE v_user_id bigint;
			DECLARE v_no_more_priv BOOLEAN DEFAULT FALSE;
			DECLARE v_cursor CURSOR FOR
            SELECT u.user_id
            FROM sb_temp.temp_user_mcl_stat AS u
            WHERE u.channel_count >= 1000
            AND u.user_id BETWEEN @start_id AND @end_id
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
				SET @uid = v_user_id;
                EXECUTE ts1 USING @uid;
                EXECUTE mcl USING @uid;
                EXECUTE ts2 USING @uid;
			END LOOP LOOP_FIRST;
		END BLOCK_SECOND;
		-- cursor end block
        SET @start_id = @end_id + 1;
    UNTIL @end_id >= @end_loop END REPEAT;
    -- SELECT @v_chunk_size, @start_loop, @end_loop;
END //
delimiter ;



KEY main_messagingchannellist_user_id_1a122f8b229c48df_idx (user_id,last_message_ts,is_hidden,count_since_joined),
KEY main_messagingchannellist_user_id_12f2a4c42c08f4ef_idx (user_id,channel_id,read_ts),  
KEY main_messagingchannellist_user_id_44927fad2c548742_idx (user_id,is_hidden,count_option),
KEY main_messagingchannellist_user_id_2e5f7ec8c21b869c_idx (user_id,is_hidden,unread_message_count,channel_id),
KEY main_messagingchannellist_user_id_2845c308b4ba493f_idx (user_id,is_hidden,pending,last_message_ts),
KEY main_messagingchannellist_user_id_3a4741ee1cc4538e_idx (user_id,last_activity_ts),
  
CREATE EVENT sb_temp.event_temp_rows_count
ON SCHEDULE AT CURRENT_TIMESTAMP DO
INSERT INTO sb_temp.temp_rows_count (cnt_type, tot_cnt)
SELECT '1st mcl rows' AS cnt_type, COUNT(*) AS tot_cnt
FROM sb_temp.poc_arch_time
;

DROP TABLE IF EXISTS sb_temp.temp_1st_mcl_res;
CREATE TABLE sb_temp.temp_1st_mcl_res (
user_id bigint NOT NULL,
diff_time time(6),
PRIMARY KEY (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
;

CREATE EVENT sb_temp.event_temp_1st_mcl_res
ON SCHEDULE AT CURRENT_TIMESTAMP DO
INSERT INTO sb_temp.temp_1st_mcl_res (user_id, diff_time)
SELECT
ts.user_id,
MAX(ts.diff_time) AS diff_time
FROM sb_temp.poc_arch_time AS ts
GROUP BY
ts.user_id
;

SELECT temp_1st_mcl_res.user_id,
    temp_1st_mcl_res.diff_time
FROM sb_temp.temp_1st_mcl_res
;
-- 405007
-- 169872643

SELECT
COUNT(*)
FROM sb_temp.temp_1st_mcl_res
WHERE diff_time > '00:00:00.999999'
;
-- 47667

SELECT ts.user_id,
    ts.start_time,
    ts.end_time,
    ts.diff_time
FROM sb_temp.poc_arch_time AS ts

;
RENAME TABLE sb_temp.test_kill_user_channel_time TO sb_temp.test_kill_user_channel_time_01;