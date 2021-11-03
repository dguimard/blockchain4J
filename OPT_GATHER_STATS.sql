CREATE OR REPLACE PACKAGE opt_gather_stats
IS
    ------------------------------------------------------------------------------------
    -- Package      : opt_gather_stats                                             --
    -- Originator   : Myra cao (huifang.cao@hp.com)                                   --
    -- Author       : Myra cao (huifang.cao@hp.com)                                   --
    -- Date         : 06-Sep-2006                                                     --
    -- Purpose      : optima tool function                                            --
    ------------------------------------------------------------------------------------
    -- Change History                                                                 --
    -- Date        Programmer        Description                                      --
    -- ----------- ----------------- ------------------------------------------------ --
    -- 13-June-2007 Myra Cao         Initial Version                                  --
    --                               optima_statistics                                --
    --                               tab_statistics                                   --
    ------------------------------------------------------------------------------------

    -- constants
    c_no_jobname_error         NUMBER := -20010;
    c_build_ind_error          NUMBER := -20011;
    c_upd_cmprs_time_error     NUMBER := -20012;
    c_gather_tbl_stats_error   NUMBER := -20013;
    --  c_tablespace             varchar2(10) := 'OPTIMA01';
    c_tablespace               VARCHAR2 (50);
    V_INDEX                    VARCHAR2 (50);

    unusable_index             EXCEPTION;
    PRAGMA EXCEPTION_INIT (unusable_index, -20000);

    -- Purpose : This procedure will map table_name by job_name from metadata table:
    PROCEDURE optima_stats (p_job_name VARCHAR2);

    -- Purpose : This procedure will collect optima tables's statitics when the table:
    -- premise : To use dynamic statistics, should have the parameter statistics_level=TYPICAL and 'GATHER_STATS_JOB' is enabled.
    -- 1) if table never has been analyzed before, p_stats_flag=1, or has refresh flag Refresh_Ind= 'Y', which means a full update,
    --  then enforce analyzing. else
    -- 2)if statistics flag stats_Ind='Y',  call dyn_tab_stats to check whether this table has been
    -- truncated or has stale data, if it's truncated or partition truncated, or has stale data changed records > 20% total records
    -- then analyzed it.

    PROCEDURE optima_stats_tbl (p_Table_Name    VARCHAR2,
                                p_Refresh_Ind   VARCHAR2 DEFAULT 'Y');

    --Purpose : gather stats for BIP Incremental table, it only gather these partitions which have been truncated.
    PROCEDURE optima_stats_ptbl (p_Table_Name   VARCHAR2,
                                 prttn_name     VARCHAR2,
                                 degree_cnt     NUMBER DEFAULT NULL);

    -- Purpose:  This procedure will gather table statitics base on dynamic view all_tab_modifications
    -- Check all_tab_modifications view, to whether this table has been truncated or has stale data, if it has.
    -- call DBMS_STATS.GATHER_TABLE_STATS to analyzed it.
    PROCEDURE dyn_tab_stats (p_user               VARCHAR2,
                             p_table_name         VARCHAR2,
                             p_estimate_percent   NUMBER,
                             p_method_opt         VARCHAR2,
                             p_change_percent     NUMBER);

    /*
    ***************************************************************************
    * Program : opt_rebuild_ind
    * Version : 1.0
    * Author  : Myra (huifang.cao@hp.com)
    * Date    : 13-Jun-2007
    * Purpose : if is_unusable = 'Y', has unusable indexes, rebuild them on table/partition/subpartition
    *           if is_unusable = 'N', default value, always rebuld indexes on table/partition/sub/partition
    */
    PROCEDURE opt_rebuild_ind (p_Table_Name         VARCHAR2,
                               p_Index_Name         VARCHAR2 DEFAULT NULL,
                               exclude_bitmap_ind   VARCHAR2 DEFAULT 'N',
                               is_unusable          VARCHAR2 DEFAULT 'N',
                               cmprs_ind            VARCHAR2 DEFAULT 'N',
                               ind_tablespace       VARCHAR2 DEFAULT NULL);

    /*
    ***************************************************************************
    * Program : opt_unuable_ind
    * Version : 1.0
    * Author  : Myra (huifang.cao@hp.com)
    * Date    : 20-May-2009
    * Purpose : unusable indexes
    ***************************************************************************
    -- 28-Sep-2009 Simon Hui  Modify                                 --
    -- Add skip_unique_flag to indicate whether to skip unique index or not
    ***************************************************************************
   */
    PROCEDURE opt_unusable_ind (p_Table_Name       VARCHAR2,
                                skip_unique_flag   VARCHAR2 DEFAULT 'N');

    PROCEDURE trck_sessn (p_obj_name    IN VARCHAR2,
                          p_obj_type    IN VARCHAR2,
                          p_step_ind    IN NUMBER,
                          p_step_desc      VARCHAR2,
                          p_ctrl_id     IN VARCHAR2);

    /*
   ***************************************************************************
   * Program : cmprs_tbl
   * Version : 1.0
   * Author  : Myra (huifang.cao@hp.com)
   * Date    : 13-March-2008
   * Purpose : if p_table_nam is null, will compress all tables wich cmprs_ind = 'Y' in table opt_tbl_metda_prc.
   *           else if p_table_name is not null, will compress this specific table
   */
    PROCEDURE cmprs_tbl (p_table_name   VARCHAR2 DEFAULT NULL,
                         p_cmprs_flg    VARCHAR2 DEFAULT 'Y');
END opt_gather_stats;
/


/* Formatted on 19/3/2019 4:21:33 PM (QP5 v5.326) */
CREATE OR REPLACE PACKAGE BODY opt_gather_stats
IS
    PROCEDURE pro_put_line (p_string IN VARCHAR2)
    IS
        v_string_length   NUMBER;
        v_string_offset   NUMBER := 1;
        v_cut_pos         NUMBER;
        v_add             NUMBER;
    BEGIN
        DBMS_OUTPUT.new_line;
        DBMS_OUTPUT.put_line (TO_CHAR (SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
        v_string_length := LENGTH (p_string);

        -- loop thru the string by 255 characters and output each chunk
        WHILE v_string_offset < v_string_length
        LOOP
            v_cut_pos :=
                  NVL (INSTR (p_string, CHR (10), v_string_offset), 0)
                - v_string_offset;

            IF v_cut_pos < 0 OR v_cut_pos >= 255
            THEN
                v_cut_pos := 255;
                v_add := 0;
            ELSE
                v_add := 1;
            END IF;

            DBMS_OUTPUT.put_line (
                SUBSTR (p_string, v_string_offset, v_cut_pos));
            v_string_offset := v_string_offset + v_cut_pos + v_add;
        END LOOP;
    END;


    PROCEDURE cmprs_tbl (p_table_name   VARCHAR2 DEFAULT NULL,
                         p_cmprs_flg    VARCHAR2 DEFAULT 'Y')
    IS
        p_is_partitioned      NUMBER := 0;
        p_is_subpartitioned   NUMBER := 0;
        p_count               NUMBER := 0;
        p_cmprs               VARCHAR2 (20);
    BEGIN
        OPT_AX_TBLSPACE (c_tablespace, V_INDEX);

        IF (p_cmprs_flg = 'Y' OR p_cmprs_flg IS NULL)
        THEN
            p_cmprs := ' COMPRESS ';
        ELSE
            IF p_cmprs_flg = 'N'
            THEN
                p_cmprs := ' NOCOMPRESS ';
            END IF;
        END IF;

        FOR i
            IN (SELECT tbl_name
                  FROM opt_tbl_mtdta_prc, user_tables
                 WHERE     tbl_name = table_name
                       AND cmprs_ind = 'Y'
                       AND NVL (cmprs_time, SYSDATE - 7) <= SYSDATE - 7
                       AND DECODE (p_table_name, NULL, 1, 0) = 1
                UNION
                SELECT table_name
                  FROM user_tables
                 WHERE table_name = UPPER (p_table_name))
        LOOP
            BEGIN
                FOR m IN (SELECT index_name
                            FROM user_indexes
                           WHERE table_name = i.tbl_name)
                LOOP
                    BEGIN
                        pro_put_line (
                               'Begin to unusable index '
                            || m.index_name
                            || ' on table '
                            || i.tbl_name);

                        EXECUTE IMMEDIATE   'alter index '
                                         || m.index_name
                                         || ' unusable ';
                    EXCEPTION
                        WHEN OTHERS
                        THEN
                            RAISE_APPLICATION_ERROR (
                                c_build_ind_error,
                                   ' Error at unusable '
                                || m.index_name
                                || ' on table '
                                || i.tbl_name
                                || SUBSTR (SQLERRM, 1, 230));
                    END;
                END LOOP;

                SELECT DECODE (partitioned, 'YES', 1, 0)
                  INTO p_is_partitioned
                  FROM user_tables
                 WHERE table_name = UPPER (i.tbl_name);

                IF (p_is_partitioned = 1)
                THEN
                    SELECT COUNT (*)
                      INTO p_count
                      FROM user_tab_subpartitions
                     WHERE table_name = UPPER (i.tbl_name);

                    IF (p_count > 0)
                    THEN
                        p_is_subpartitioned := 1;
                    END IF;
                END IF;

                IF (p_is_partitioned = 0)
                THEN
                    BEGIN
                        pro_put_line (
                               'Begin to move table '
                            || i.tbl_name
                            || ' to tablespace '
                            || c_tablespace
                            || ' by compress/uncompress');

                        EXECUTE IMMEDIATE   'alter table '
                                         || i.tbl_name
                                         || ' move '
                                         || c_tablespace
                                         || p_cmprs;
                    EXCEPTION
                        WHEN OTHERS
                        THEN
                            RAISE_APPLICATION_ERROR (
                                c_build_ind_error,
                                   ' Error at move table '
                                || i.tbl_name
                                || ' to tablespace by '
                                || p_cmprs
                                || SUBSTR (SQLERRM, 1, 230));
                    END;
                ELSE
                    IF (p_is_partitioned = 1) AND (p_is_subpartitioned = 0)
                    THEN
                        BEGIN
                            pro_put_line (
                                   'Begin to move table '
                                || i.tbl_name
                                || ' to tablespace '
                                || c_tablespace
                                || ' by '
                                || p_cmprs);

                            FOR j IN (SELECT partition_name
                                        FROM user_tab_partitions
                                       WHERE table_name = i.tbl_name)
                            LOOP
                                BEGIN
                                    EXECUTE IMMEDIATE   'alter table '
                                                     || i.tbl_name
                                                     || ' move partition '
                                                     || j.partition_name
                                                     || c_tablespace
                                                     || p_cmprs;

                                    pro_put_line (
                                           'Begin alter table '
                                        || i.tbl_name
                                        || ' move partition '
                                        || j.partition_name
                                        || ' tablespace '
                                        || c_tablespace
                                        || p_cmprs);
                                EXCEPTION
                                    WHEN OTHERS
                                    THEN
                                        RAISE_APPLICATION_ERROR (
                                            c_build_ind_error,
                                               ' Error at move table '
                                            || i.tbl_name
                                            || ' partition '
                                            || j.partition_name
                                            || ' to tablespace by '
                                            || p_cmprs
                                            || SUBSTR (SQLERRM, 1, 230));
                                END;
                            END LOOP;
                        END;
                    ELSE
                        IF     (p_is_partitioned = 1)
                           AND (p_is_subpartitioned = 1)
                        THEN
                            BEGIN
                                EXECUTE IMMEDIATE   'alter table '
                                                 || i.tbl_name
                                                 || p_cmprs;

                                pro_put_line (
                                       'Begin alter table '
                                    || i.tbl_name
                                    || p_cmprs);

                                FOR k IN (SELECT subpartition_name
                                            FROM user_tab_subpartitions
                                           WHERE table_name = i.tbl_name)
                                LOOP
                                    BEGIN
                                        EXECUTE IMMEDIATE   'alter table '
                                                         || i.tbl_name
                                                         || ' move subpartition '
                                                         || k.subpartition_name
                                                         || c_tablespace;

                                        pro_put_line (
                                               'Begin alter table '
                                            || i.tbl_name
                                            || ' move subpartition '
                                            || k.subpartition_name
                                            || ' tablespace '
                                            || c_tablespace);
                                    EXCEPTION
                                        WHEN OTHERS
                                        THEN
                                            RAISE_APPLICATION_ERROR (
                                                c_build_ind_error,
                                                   ' Error at move table '
                                                || i.tbl_name
                                                || ' subpartition '
                                                || k.subpartition_name
                                                || ' to tablespace '
                                                || SUBSTR (SQLERRM, 1, 230));
                                    END;
                                END LOOP;
                            EXCEPTION
                                WHEN OTHERS
                                THEN
                                    RAISE_APPLICATION_ERROR (
                                        c_build_ind_error,
                                           ' Error at alter table '
                                        || i.tbl_name
                                        || p_cmprs
                                        || SUBSTR (SQLERRM, 1, 230));
                            END;
                        END IF;
                    END IF;
                END IF;

                BEGIN
                    pro_put_line (
                        'Begin to rebulid indexes on ' || i.tbl_name || ';');
                    opt_rebuild_ind (i.tbl_name,
                                     NULL,
                                     'N',
                                     'N',
                                     'Y');
                EXCEPTION
                    WHEN OTHERS
                    THEN
                        RAISE_APPLICATION_ERROR (
                            c_build_ind_error,
                               ' Error at rebuilding indexes of table '
                            || i.tbl_name
                            || ' '
                            || SUBSTR (SQLERRM, 1, 230));
                END;

                BEGIN
                    pro_put_line (
                           'Begin to gather statistics on table '
                        || i.tbl_name
                        || ';');
                    optima_stats_tbl (i.tbl_name, 'Y');
                EXCEPTION
                    WHEN OTHERS
                    THEN
                        RAISE_APPLICATION_ERROR (
                            c_build_ind_error,
                               ' Error at regather statistics of table '
                            || i.tbl_name
                            || ' '
                            || SUBSTR (SQLERRM, 1, 230));
                END;
            END;

            BEGIN
                UPDATE opt_tbl_mtdta_prc
                   SET cmprs_time = SYSDATE
                 WHERE tbl_name = i.tbl_name;

                COMMIT;
            EXCEPTION
                WHEN OTHERS
                THEN
                    RAISE_APPLICATION_ERROR (
                        c_upd_cmprs_time_error,
                           ' Error at update comress/uncompress time for '
                        || i.tbl_name
                        || ' '
                        || SUBSTR (SQLERRM, 1, 230));
            END;
        END LOOP;
    END;

    /**********************************************************************/
    /* name: trck_sessn                                                                                    */
    /* purpose: track session statistics                                 */
    /* parameters:                                                                                           */
    /*           p_obj_name: object name for tracking                    */
    /*           p_obj_type: object type for tracking                    */
    /*           p_step_ind: step indicator                              */
    /*           p_ctrl_id : control order id                            */
    /* version: 1.00 - initial version                                                         */
    /**********************************************************************/
    PROCEDURE trck_sessn (p_obj_name    IN VARCHAR2,
                          p_obj_type    IN VARCHAR2,
                          p_step_ind    IN NUMBER,
                          p_step_desc   IN VARCHAR2,
                          p_ctrl_id     IN VARCHAR2)
    IS
        v_trck_tbl_rec     opt_prcss_mtdta_prc%ROWTYPE;

        /* Added below variable by venkatesh.K - to handle deadlock - 24AUG2018 */
        v_sid              NUMBER := 0;
        v_event_name       VARCHAR2 (200);
        v_event_st_val     VARCHAR2 (200);
        v_temp_cnt         NUMBER := 0;

        --EXCEPTIONS
        EXP_ORA_DEADLOCK   EXCEPTION;
        PRAGMA EXCEPTION_INIT (EXP_ORA_DEADLOCK, -60);
    BEGIN
        -- Checking paramters
        pro_put_line ('Checking paramters!');

        IF    p_obj_name IS NULL
           OR p_obj_type IS NULL
           OR p_step_ind IS NULL
           OR p_ctrl_id IS NULL
        THEN
            pro_put_line ('Error: wrong input parameters!');
            raise_application_error (-20006,
                                     'Error: wrong input parameters!');
        END IF;


        -- Checking metadata status, whether enable tracking
        BEGIN
            SELECT *
              INTO v_trck_tbl_rec
              FROM opt_prcss_mtdta_prc
             WHERE TBL_NAME = p_obj_name AND TRCK_IND = 'Y';
        EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
                pro_put_line (
                       'Warning: no target object_name, object_type, steps with '''
                    || p_obj_name
                    || ''' registered in table OPT_TRCK_OBJ_PRC or tracking isn''t enabled');
                RETURN;
            WHEN OTHERS
            THEN
                raise_application_error (
                    -20009,
                       'Error: wrong metadata in opt_prcss_mtdta_prc table!--'
                    || SQLERRM);
        END;

        /* Added below selection by venkatesh.K - to handle deadlock - 24-Aug-2018 - CHG0126767 */
        BEGIN
            SELECT COUNT (1) INTO v_temp_cnt FROM OPT_TRCK_STATS_LOG_TEMP;

            IF v_temp_cnt > 0
            THEN
                INSERT INTO OPT_TRCK_STATS_LOG
                    SELECT * FROM OPT_TRCK_STATS_LOG_TEMP;

                COMMIT;

                pro_put_line (
                    'Loading data from OPT_TRCK_STATS_LOG_TEMP to OPT_TRCK_STATS_LOG Table ');

                DELETE FROM OPT_TRCK_STATS_LOG_TEMP;

                COMMIT;
                v_temp_cnt := 0;

                pro_put_line (
                    'Deleting data from OPT_TRCK_STATS_LOG_TEMP Table ');
            END IF;
        EXCEPTION
            WHEN OTHERS
            THEN
                pro_put_line (
                       'Error when Deleting data from OPT_TRCK_STATS_LOG_TEMP Table - '
                    || SQLERRM);
                ROLLBACK;
        END;

        /* end of selection by venkatesh.K - to handle deadlock - 24-Aug-2018 - CHG0126767 */


        pro_put_line ('Session tracking ' || p_step_desc);

        BEGIN
            MERGE INTO OPT_TRCK_STATS_LOG LOG
                 USING (SELECT p_obj_name      AS p_obj_name,
                               p_obj_type      AS p_obj_type,
                               p_step_ind      AS p_step_ind,
                               p_step_desc     AS p_step_desc,
                               p_ctrl_id       AS p_ctrl_id,
                               s.sid,
                               n.name          p_name,
                               s.VALUE         p_value
                          FROM sys.V_$SESSTAT s, sys.V_$STATNAME n
                         WHERE     s.sid =
                                   (SELECT SYS_CONTEXT ('USERENV', 'SID')
                                      FROM DUAL)
                               AND s.statistic# = n.statistic#
                               AND n.name IN (SELECT EVENT_NAME
                                                FROM OPT_TRCK_EVENT_PRC
                                               WHERE obj_type = p_obj_type))
                       t
                    ON (    LOG.obj_name = t.p_obj_name
                        AND LOG.obj_type = t.p_obj_type
                        AND LOG.step_ind = t.p_step_ind
                        AND LOG.event_name = t.p_name
                        AND LOG.ctrlm_ordr_id = t.p_ctrl_id)
            WHEN MATCHED
            THEN
                UPDATE SET
                    LOG.END_TIME = SYSDATE,
                    LOG.ELPSD_TIME =
                        ROUND ((SYSDATE - LOG.START_TIME) * 24 * 60 * 60),
                    LOG.EVENT_END_VAL = t.p_value,
                    LOG.EVENT_DELTA_VAL = t.p_value - LOG.EVENT_START_VAL
            WHEN NOT MATCHED
            THEN
                INSERT     (LOG.trck_log_id,
                            LOG.obj_name,
                            LOG.obj_type,
                            LOG.step_ind,
                            LOG.step_desc,
                            LOG.ctrlm_ordr_id,
                            LOG.sessn_sid,
                            LOG.start_time,
                            LOG.event_name,
                            LOG.event_start_val)
                    VALUES (OPT_TRCK_LOG_ID_SEQ.NEXTVAL,
                            t.p_obj_name,
                            t.p_obj_type,
                            t.p_step_ind,
                            t.p_step_desc,
                            t.p_ctrl_id,
                            t.sid,
                            SYSDATE,
                            t.p_name,
                            t.p_value);

            COMMIT;
        EXCEPTION
            /* Added below selection by venkatesh.K - to handle deadlock - 24Aug2018 -- CHG0126767 */
            WHEN EXP_ORA_DEADLOCK
            THEN
                pro_put_line (
                       'Error: when update records into table OPT_TRCK_STATS_LOG - Loading to TEMP! --'
                    || SQLERRM);
                ROLLBACK;


                SELECT s.sid, n.name, s.VALUE
                  INTO v_sid, v_event_name, v_event_st_val
                  FROM sys.V_$SESSTAT s, sys.V_$STATNAME n
                 WHERE     s.sid =
                           (SELECT SYS_CONTEXT ('USERENV', 'SID') FROM DUAL)
                       AND s.statistic# = n.statistic#
                       AND n.name IN (SELECT EVENT_NAME
                                        FROM OPT_TRCK_EVENT_PRC
                                       WHERE obj_type = p_obj_type)
                       AND ROWNUM < 2;


                SELECT SYS_CONTEXT ('USERENV', 'SID') INTO v_sid FROM DUAL;


                INSERT INTO OPT_TRCK_STATS_LOG_TEMP (trck_log_id,
                                                     obj_name,
                                                     obj_type,
                                                     step_ind,
                                                     step_desc,
                                                     ctrlm_ordr_id,
                                                     sessn_sid,
                                                     start_time,
                                                     event_name,
                                                     event_start_val,
                                                     END_TIME,
                                                     ELPSD_TIME,
                                                     EVENT_END_VAL)
                     VALUES (OPT_TRCK_LOG_ID_SEQ.NEXTVAL,
                             p_obj_name,
                             p_obj_type,
                             p_step_ind,
                             p_step_desc,
                             p_ctrl_id,
                             v_sid,
                             SYSDATE,
                             v_event_name,
                             v_event_st_val,
                             SYSDATE,
                             NULL,
                             'TEMP TBL');


                pro_put_line (
                    'Deadlock exception handling - Inserting data to OPT_TRCK_STATS_LOG_TEMP Table ');
            /* end of selection by venkatesh.K - to handle deadlock - 24Aug2018 - CHG0126767 */

            WHEN OTHERS
            THEN
                pro_put_line (
                       'Error: when update records into table OPT_TRCK_STATS_LOG! --'
                    || SQLERRM);
                ROLLBACK;
                raise_application_error (
                    -20008,
                    'Error: when update records into table OPT_TRCK_STATS_LOG!');
        END;

        --pro_put_line ('End Session tracking for '||p_step_desc);
        COMMIT;
    END trck_sessn;


    PROCEDURE optima_stats (p_job_name VARCHAR2)
    IS
        /*
        ***************************************************************************
        * Program : optima_stats
        * Version : 1.0
        * Author  : Myra (huifang.cao@hp.com)
        * Date    : 13-Jun-2007
        * Purpose : collect stale optima tables's statitics
        * Parameters : Job_name (controlM job name)
        * Process:
        * 1.   get some information e.g. user, table_name and refresh_ind, stats_ind        --
        * 2.   if table_name is not null or v_stats_ind = 'Y' then call optima_stats_tbl to check whether analytics should be done
        * 3.   if it's a DIM table, and has relevant FDIM table, then gather statstics on FDIM table also.
        * Change History
        * Date         Programmer         Description
        * -------------------------------------------------------------------------
        * 22-Jun-2007   Myra              Initial Version
        * 26-Feb 2008   Myra        improvement
        ****************************************************************************
        */
        Job_Name        OPT_PRCSS_MTDTA_PRC.JOB_NAME%TYPE;
        v_Table_Name    OPT_PRCSS_MTDTA_PRC.TBL_NAME%TYPE;
        v_Stats_Ind     OPT_PRCSS_MTDTA_PRC.STATS_IND%TYPE;
        v_Refresh_Ind   OPT_PRCSS_MTDTA_PRC.RFRSH_IND%TYPE;
        v_fdim          OPT_PRCSS_MTDTA_PRC.TBL_NAME%TYPE;
        v_count         NUMBER;
    BEGIN
        pro_put_line (
            'Checking Optima Jobs to gather statstics for relevant tables(begin)');

        --step 1
        --get table name, region name, statistics flag, refresh flag from metadata table OPT_PRCSS_MTDTA_PRC by job_name
        BEGIN
            SELECT DISTINCT JOB_NAME,
                            TBL_NAME,
                            STATS_IND,
                            RFRSH_IND
              INTO Job_Name,
                   v_Table_Name,
                   v_Stats_Ind,
                   v_Refresh_Ind
              FROM OPT_PRCSS_MTDTA_PRC
             WHERE UPPER (JOB_NAME) = UPPER (p_job_name);
        EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
                pro_put_line (
                       'Error: Job name doesn''t '
                    || p_job_name
                    || ' exist in metadata table OPT_PRCSS_MTDTA_PRC, please check!');
                raise_application_error (
                    c_no_jobname_error,
                    ' Error: Job name doesn''t exist in metadata table OPT_PRCSS_MTDTA_PRC, please check!');
        END;

        --step 2
        -- if table_name is not null, or v_stats_ind = 'Y',  means this job has relevant table need to gather statstics, otherwise return.

        IF ((v_Table_Name IS NOT NULL) AND (v_Stats_Ind = 'Y'))
        THEN
            -- Modified by Myra in R8. for always gather statistics for optima tables.
            -- optima_stats_tbl(v_Table_Name, v_Refresh_Ind);
            optima_stats_tbl (v_Table_Name);

            --step 3
            -- if it's a DIM table, and has relevant FDIM table, then gather statstics on FDIM table also.
            SELECT COUNT (*)
              INTO v_count
              FROM user_tables
             WHERE     table_name = REPLACE (v_Table_Name, '_DIM', '_FDIM')
                   AND table_name LIKE '%_DIM';

            IF (v_count >= 1)
            THEN
                SELECT table_name
                  INTO v_fdim
                  FROM user_tables
                 WHERE table_name = REPLACE (v_Table_Name, '_DIM', '_FDIM');

                IF (v_fdim IS NOT NULL)
                THEN
                    -- Modified by Myra in R8. for always gather statistics for optima tables.
                    -- optima_stats_tbl(v_fdim, v_Refresh_Ind);
                    optima_stats_tbl (v_fdim);
                END IF;
            END IF;
        ELSE
            pro_put_line (
                'No relevant table name or Stats_Flag is not ''Y''');
        END IF;

        pro_put_line (
            'Checking Optima Jobs to gather statstics for relevant tables(end)');
    END optima_stats;

    PROCEDURE optima_stats_tbl (p_Table_Name    VARCHAR2,
                                p_Refresh_Ind   VARCHAR2 DEFAULT 'Y')
    IS
        /*
        ***************************************************************************
        * Program : optima_stats_tbl
        * Version : 1.0
        * Author  : Myra (huifang.cao@hp.com)
        * Date    : 13-Jun-2007
        * Purpose : collect stale optima tables's statitics
        * Parameters : Job_name (controlM job name)
        * Process:
        * 1.   get some information e.g. user    --
        * 2.   check if this table have been partitioned. if it has, get partition name.
        * 3.   Check whether this table/partition has been analyzed, if it hasn't, then set p_stats_flag=1
        * 4.1  if  REFRESH_IND = 'Y' || p_stats_flag=1 then always analyzing the table.
        * 4.2  if STATS_IND='Y'  then call dyn_tab_stats to check whether analytics should be done
        * Change History
        * Date         Programmer         Description
        * -------------------------------------------------------------------------
        * 22-Jun-2007   Myra              Initial Version
        ****************************************************************************
        */
        p_user                   VARCHAR2 (30);
	    V_SQL             VARCHAR2 (32767);
	    V_DEGREE            NUMBER := 0; -- performance fix by DXC for Jira#OP-2195
        p_Stats_Methd_Optn       OPT_TBL_MTDTA_PRC.STATS_METHD_OPTN%TYPE
                                     := 'FOR ALL INDEXED COLUMNS SIZE AUTO';
        p_Stats_Estmt_Pct        OPT_TBL_MTDTA_PRC.STATS_ESTMT_PCT%TYPE := 10;
        p_Stats_Chng_Pct_Tlrnc   OPT_TBL_MTDTA_PRC.STATS_CHNG_PCT_TLRNC%TYPE
                                     := 20;

        v_Refresh_Ind            OPT_PRCSS_MTDTA_PRC.RFRSH_IND%TYPE;
        v_Stats_Methd_Optn       OPT_TBL_MTDTA_PRC.STATS_METHD_OPTN%TYPE;
        v_Stats_Estmt_Pct        OPT_TBL_MTDTA_PRC.STATS_ESTMT_PCT%TYPE;
        v_Stats_Chng_Pct_Tlrnc   OPT_TBL_MTDTA_PRC.STATS_CHNG_PCT_TLRNC%TYPE;

        -- whether the table has been analyzed before. p_stats_flag=1 never been analyzed. p_stats_flag=0. has been analyzed before.
        p_stats_flag             NUMBER := 0;
        -- whether the table is partitioned. p_is_partitioned=1, it's a partitioned table, p_is_partitioned=0, it isn't a partitioned table.
        v_is_partitioned         NUMBER := 0;

        -- p_partition_num means the number of partitions that haven't be analyzed.
        v_partition_num          NUMBER := 0;
    BEGIN
        --step 1
        --get user of tables, and parameters for gathering stats
        SELECT username
          INTO p_user
          FROM user_users
         WHERE ROWNUM = 1;
		--Start Added by DXC for performance fix Jira#OP-2195 
		IF SUBSTR(UPPER(USER),(INSTR(UPPER(USER),'_',1,2) + 1),2) = 'AP' THEN
			V_DEGREE := 16;
		ELSE
			V_DEGREE := 8;
		END IF;
		--End Added by DXC for performance fix Jira#OP-2195

        pro_put_line (
            'Checking parameters used when gathering stats (begin)');

        BEGIN
            v_Refresh_Ind := p_Refresh_Ind;

              SELECT MAX (STATS_METHD_OPTN),
                     MAX (STATS_ESTMT_PCT),
                     MAX (STATS_CHNG_PCT_TLRNC)
                INTO v_Stats_Methd_Optn,
                     v_Stats_Estmt_Pct,
                     v_Stats_Chng_Pct_Tlrnc
                FROM OPT_TBL_MTDTA_PRC
               WHERE UPPER (TBL_NAME) = UPPER (p_Table_Name)
            GROUP BY TBL_Name;
        EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
                pro_put_line (
                       'Wrong: table '
                    || p_Table_Name
                    || ' doesn''t exist in OPT_TBL_MTDTA_PRC, default parameters will be used!');
                v_Stats_Methd_Optn := p_Stats_Methd_Optn;
                v_Stats_Estmt_Pct := p_Stats_Estmt_Pct;
                v_Stats_Chng_Pct_Tlrnc := p_Stats_Chng_Pct_Tlrnc;
        END;

        pro_put_line ('Checking parameters used when gathering stats (End)');

        --step 2
        --if table_name is not null, check if this table have been partitioned. if it has, get partition name.

        BEGIN
            pro_put_line ('Checking partition/subpartition (begin)');

            SELECT DECODE (partitioned, 'YES', 1, 0)
              INTO v_is_partitioned
              FROM user_tables
             WHERE table_name = UPPER (p_Table_Name);
        EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
                pro_put_line (
                       'Error: table '
                    || p_Table_Name
                    || ' doesn''t exist, please check!');
                raise_application_error (
                    c_no_jobname_error,
                    ' Error: table doesn''t exist, please check!');
        END;


        IF (v_is_partitioned = 0)
        THEN
            --step 3 Check whether this table/partition has been analyzed, if it hasn't, then always analyzed it
            --table is not partition tables. check last_analyzed in all_tables. if it's null, set p_stats_flag=1.
            SELECT TO_NUMBER (DECODE (last_analyzed, NULL, 1, 0))
              INTO p_stats_flag
              FROM user_tables
             WHERE table_name = UPPER (p_Table_Name) AND ROWNUM = 1;
        ELSE
            --table is partition tables. check last_analyzed in all_tab_partitions. if it's null, set p_stats_flag=1.
            SELECT COUNT (*)
              INTO v_partition_num
              FROM user_tab_partitions p
             WHERE     p.table_name = UPPER (p_Table_Name)
                   AND p.last_analyzed IS NULL;

            IF (v_partition_num > 0)
            THEN
                p_stats_flag := 1;
            END IF;
        END IF;

        pro_put_line ('Checking partition/subpartition (end)');

        --step 4.1
        -- specifical processing for never analyzed tables (enfored analysis is taken)

        IF (p_stats_flag = 1 OR v_Refresh_Ind = 'Y')
        THEN
            pro_put_line (
                'gather statistics on table which has not been analyzed or full updated (begin)');
		       --Start Added by DXC for performance fix Jira#OP-2195
			IF SUBSTR(UPPER(USER),(INSTR(UPPER(USER),'_',1,2) + 1),2) = 'AP' THEN
			V_SQL := 'COMMIT';
                    	PRO_PUT_LINE (V_SQL);
                     	EXECUTE IMMEDIATE V_SQL;

			V_SQL := 'ALTER SESSION FORCE PARALLEL QUERY PARALLEL 16';
                    	PRO_PUT_LINE (V_SQL);
                     	EXECUTE IMMEDIATE V_SQL;
		        END IF;
        		--End Added by DXC for performance fix Jira#OP-2195
            BEGIN
                DBMS_STATS.GATHER_TABLE_STATS (
                    ownname            => p_user,
                    tabname            => p_Table_Name,
                    partname           => NULL             /*p_regn_partname*/
                                              ,
                    --estimate_percent => v_Stats_Estmt_Pct, --commented by Rajesh J for fine tuning on 9-Oct-2012 for CR C02251989
                    estimate_percent   => DBMS_STATS.AUTO_SAMPLE_SIZE, --added by Rajesh J for fine tuning on 9-Oct-2012  for CR C02251989
                    --method_opt       => v_Stats_Methd_Optn,--commented by Rajesh J for fine tuning on 17-Oct-2012 for CR C02251989
                    --method_opt       => 'FOR ALL COLUMNS SIZE 1', --added by Rajesh J for fine tuning on 17-Oct-2012 for CR C02251989
                    method_opt         => 'FOR ALL COLUMNS SIZE AUTO', --added by Rajesh J for fine tuning on 25-Oct-2012 for CR C02251989
                    degree             => V_DEGREE, --added by Rajesh J for fine tuning on 4-Oct-2012 for CR C02251989 --Added by DXC for performance fix Jira#OP-2195
                    granularity        => 'AUTO',
                    CASCADE            => TRUE);
     
	 
	 --Start Added by DXC for performance fix Jira#OP-2195
            IF SUBSTR(UPPER(USER),(INSTR(UPPER(USER),'_',1,2) + 1),2) = 'AP' THEN
			pro_put_line ('Commit');
			V_SQL := 'COMMIT';
                    	PRO_PUT_LINE (V_SQL);
                     	EXECUTE IMMEDIATE V_SQL;
			V_SQL := 'ALTER SESSION disable PARALLEL QUERY';
                    	PRO_PUT_LINE (V_SQL);
                     	EXECUTE IMMEDIATE V_SQL;
			pro_put_line (
                            'start to gather statistics on table (end-ok)');
			END IF;
	  --End Added by DXC for performance fix Jira#OP-2195	
            EXCEPTION
                WHEN unusable_index
                THEN
                    BEGIN
                        pro_put_line (
                            'warning: meet unusable_index on this table, rebuild it');
                        opt_rebuild_ind (p_table_name,
                                         NULL,
                                         'N',
                                         'Y');
                        pro_put_line (
                            'start to gather statistics on table (begin)');
         --Start Added by DXC for performance fix Jira#OP-2195
            IF SUBSTR(UPPER(USER),(INSTR(UPPER(USER),'_',1,2) + 1),2) = 'AP' THEN
			V_SQL := 'COMMIT';
                    	PRO_PUT_LINE (V_SQL);
                     	EXECUTE IMMEDIATE V_SQL;

			V_SQL := 'ALTER SESSION FORCE PARALLEL QUERY PARALLEL 16';
                    	PRO_PUT_LINE (V_SQL);
                     	EXECUTE IMMEDIATE V_SQL;
            END IF;
         --End Added by DXC for performance fix Jira#OP-2195
            

                        DBMS_STATS.GATHER_TABLE_STATS (
                            ownname            => p_user,
                            tabname            => p_Table_Name,
                            partname           => NULL     /*p_regn_partname*/
                                                      ,
                            --estimate_percent => v_Stats_Estmt_Pct,--commented by Rajesh J for fine tuning on 9-Oct-2012 for CR C02251989
                            estimate_percent   => DBMS_STATS.AUTO_SAMPLE_SIZE, --added by Rajesh J for fine tuning on 9-Oct-2012  for CR C02251989
                            --method_opt       => v_Stats_Methd_Optn,--commented by Rajesh J for fine tuning on 17-Oct-2012 for CR C02251989
                            --method_opt       => 'FOR ALL COLUMNS SIZE 1', --added by Rajesh J for fine tuning on 17-Oct-2012 for CR C02251989
                            method_opt         => 'FOR ALL COLUMNS SIZE AUTO', --added by Rajesh J for fine tuning on 25-Oct-2012 for CR C02251989
                            degree             => V_DEGREE, --added by Rajesh J for fine tuning on 4-Oct-2012 for CR C02251989 --Added by DXC for performance fix Jira#OP-2195
                            granularity        => 'AUTO',
                            CASCADE            => TRUE);
 
             --Start Added by DXC for performance fix Jira#OP-2195
            IF SUBSTR(UPPER(USER),(INSTR(UPPER(USER),'_',1,2) + 1),2) = 'AP' THEN
			pro_put_line ('Commit');
			V_SQL := 'COMMIT';
                    	PRO_PUT_LINE (V_SQL);
                     	EXECUTE IMMEDIATE V_SQL;
			V_SQL := 'ALTER SESSION disable PARALLEL QUERY';
                    	PRO_PUT_LINE (V_SQL);
                     	EXECUTE IMMEDIATE V_SQL;
	        END IF;
		    --End Added by DXC for performance fix Jira#OP-2195

			pro_put_line (
                            'start to gather statistics on table (end-ok)');

                    EXCEPTION
                        WHEN OTHERS
                        THEN
                            RAISE_APPLICATION_ERROR (
                                c_gather_tbl_stats_error,
                                   ' Error at gather table stats'
                                || SUBSTR (SQLERRM, 1, 230));
                    END;
                WHEN OTHERS
                THEN
                    RAISE_APPLICATION_ERROR (
                        c_gather_tbl_stats_error,
                           ' Error at gather table stats'
                        || SUBSTR (SQLERRM, 1, 230));
            END;

            pro_put_line (
                'gather statistics on table which has not been analyzed or full updated  (end)');
        ELSE
            --step 4.2
            -- STATS_IND='Y'  then call dyn_tab_stats to check whether analytics should be done
            pro_put_line (
                'dynamic gather statistics on table according to changes (begin)');
            dyn_tab_stats (p_user,
                           p_Table_Name,
                           v_Stats_Estmt_Pct,
                           v_Stats_Methd_Optn,
                           v_Stats_Chng_Pct_Tlrnc);
            pro_put_line (
                'dynamic gather statistics on table according to changes (end)');
        END IF;
    END optima_stats_tbl;

    PROCEDURE dyn_tab_stats (p_user               VARCHAR2,
                             p_table_name         VARCHAR2,
                             p_estimate_percent   NUMBER,
                             p_method_opt         VARCHAR2,
                             p_change_percent     NUMBER)
    IS
        /*
        ***************************************************************************
        * Program : dyn_tab_stats
        * Version : 1.0
        * Author  : Myra (huifang.cao@hp.com)
        * Date    : 13-Jun-2007
        * Purpose : collect stale optima tables's statitics
        * Parameters : p_user
        *              p_table_name
        *              p_partname
        *              p_estimate_percent
        *              p_method_opt
        * Process:
        * 1. Manual fresh table changes in dymanical wiew all_tab_modifications.
        * 2. check (USER_TAB_MODIFICATIONS.TRUNCATED = 'YES') OR
        *               (USER_TABLES.NUM_ROWS IS NULL) OR
        *                (USER_TAB_MODIFICATIONS.INSERTS + USER_TAB_MODIFICATIONS.UPDATES + USER_TAB_MODIFICATIONS.DELETES >= USER_TABLES.NUM_ROWS * 0.2 )
        *    if table is partitioned, use user_tab_partition while not all_tables in formula
        * 3.if p_stale_flag = 1, then gather stats for the table/partition
        * Change History
        * Date         Programmer         Description
        * -------------------------------------------------------------------------
        * 30-OCT-2005   Myra              Initial Version
        * 14-Sep-2010   Leo               Remove the step of flush database monitoring info
        ****************************************************************************
        */
        p_stale_flag       NUMBER := 0;
        p_num_rows         ALL_TABLES.NUM_ROWS%TYPE;
        p_last_analyzed    ALL_TABLES.LAST_ANALYZED%TYPE;
        p_is_partitioned   NUMBER := 0;
        v_cnctn_info       OPT_CNCTN_INFO_PRC.CNCTN_VAL%TYPE;
		V_DEGREE           NUMBER :=0;  --Added by DXC for performance fix Jira#OP-2195
    BEGIN
        --step 1
        -- Manual fresh table changes in dymanical wiew all_tab_modifications.
        --pro_put_line('start flush database monitoring info (begin)');

        -- Change Request #1336
        /*select substr(opt_tool.FN_Decrypt(CNCTN_VAL,'optimaco'),1,
                      instr(opt_tool.FN_Decrypt(CNCTN_VAL,'optimaco'),'#')-1)
        into v_cnctn_info
          from OPT_CNCTN_INFO_PRC
          where upper(trim(CNCTN_NAME_CODE)) = 'ADW1D';

        v_cnctn_info := substr(v_cnctn_info,1,instr(v_cnctn_info,'@')-1);

        for x in (
              select substr(INST_NAME,instr(INST_NAME,':')+1,
                            length(INST_NAME)-instr(INST_NAME,':'))
              as inst_name
              from V$ACTIVE_INSTANCES
        )
        loop
           conn v_cnctn_info@x.inst_name;
           dbms_stats.flush_database_monitoring_info;
        end loop;*/
        --removed by Leo on Sep 14th, 2010, R11 begin
        --dbms_stats.flush_database_monitoring_info;
        --pro_put_line('start flush database monitoring info (end)');

        --step 2
        -- If table isn't partitioned table or it's a partition table but not partitioned by region code, check in all_tables for comparing records change,
        -- if table is truncated, or changed records > 20% total records (by last analyzed in all_tables. gather stats for this table
        --removed by Leo on Sep 14th, 2010, R11 end
       	
		--Start Added by DXC for performance fix Jira#OP-2195 
		IF SUBSTR(UPPER(USER),(INSTR(UPPER(USER),'_',1,2) + 1),2) = 'AP' THEN
			V_DEGREE := 16;
		ELSE
			V_DEGREE := 8;
		END IF;
		--End Added by DXC for performance fix Jira#OP-2195
		
        SELECT DECODE (partitioned, 'YES', 1, 0)
          INTO p_is_partitioned
          FROM user_tables
         WHERE table_name = UPPER (p_Table_Name);

        IF (p_is_partitioned = 0)
        THEN
            SELECT COUNT (*)
              INTO p_stale_flag
              FROM user_tab_modifications m
             WHERE     m.table_name = UPPER (UPPER (p_Table_Name))
                   AND m.truncated = 'YES';

            IF (p_stale_flag < 1)
          
            END IF;

            IF (p_stale_flag >= 1)
            THEN
                BEGIN
                    pro_put_line (
                           'start to gather statistics on table'
                        || p_table_name
                        || ' (begin)');
                    DBMS_STATS.GATHER_TABLE_STATS (
                        ownname            => p_user,
                        tabname            => p_table_name,
                        partname           => NULL,
                        --estimate_percent => p_estimate_percent, --commented by Rajesh J for fine tuning on 9-Oct-2012 for CR C02251989
                        estimate_percent   => DBMS_STATS.AUTO_SAMPLE_SIZE, --added by Rajesh J for fine tuning on 9-Oct-2012 for CR C02251989
                        --method_opt       => p_method_opt,--commented by Rajesh J for fine tuning on 17-Oct-2012 for CR C02251989
                        --method_opt       => 'FOR ALL COLUMNS SIZE 1', --added by Rajesh J for fine tuning on 17-Oct-2012 for CR C02251989
                        method_opt         => 'FOR ALL COLUMNS SIZE AUTO', --added by Rajesh J for fine tuning on 25-Oct-2012 for CR C02251989
                        degree             => V_DEGREE, --added by Rajesh J for fine tuning on 4-Oct-2012 for CR C02251989 --Added by DXC for performance fix Jira#OP-2195
                        granularity        => 'AUTO',
                        CASCADE            => TRUE);

                    pro_put_line (
                           'start to gather statistics on table '
                        || p_table_name
                        || ' (end-ok)');
                EXCEPTION
                    WHEN unusable_index
                    THEN
                        BEGIN
                            pro_put_line (
                                   'warning: meet unusable_index on  table '
                                || p_table_name
                                || ', rebuild it');
                            opt_rebuild_ind (p_table_name,
                                             NULL,
                                             'N',
                                             'Y');
                            pro_put_line (
                                   'start to gather statistics on table '
                                || p_table_name
                                || ' (begin)');
                            DBMS_STATS.GATHER_TABLE_STATS (
                                ownname       => p_user,
                                tabname       => p_table_name,
                                partname      => NULL,
                                --estimate_percent => p_estimate_percent,--commented by Rajesh J for fine tuning on 9-Oct-2012 for CR C02251989
                                estimate_percent   =>
                                    DBMS_STATS.AUTO_SAMPLE_SIZE, --added by Rajesh J for fine tuning on 9-Oct-2012 for CR C02251989
                                --method_opt       => p_method_opt,--commented by Rajesh J for fine tuning on 17-Oct-2012 for CR C02251989
                                --method_opt       => 'FOR ALL COLUMNS SIZE 1', --added by Rajesh J for fine tuning on 17-Oct-2012 for CR C02251989
                                method_opt    => 'FOR ALL COLUMNS SIZE AUTO', --added by Rajesh J for fine tuning on 25-Oct-2012 for CR C02251989
                                degree        => V_DEGREE, --added by Rajesh J for fine tuning on 4-Oct-2012 for CR C02251989 --Added by DXC for performance fix Jira#OP-2195
                                granularity   => 'AUTO',
                                CASCADE       => TRUE);
                            pro_put_line (
                                   'start to gather statistics on table '
                                || p_table_name
                                || '  (end-ok)');
                        EXCEPTION
                            WHEN OTHERS
                            THEN
                                RAISE_APPLICATION_ERROR (
                                    c_gather_tbl_stats_error,
                                       ' Error at gather table stats'
                                    || SUBSTR (SQLERRM, 1, 230));
                        END;
                    WHEN OTHERS
                    THEN
                        RAISE_APPLICATION_ERROR (
                            c_gather_tbl_stats_error,
                               ' Error at gather table stats'
                            || SUBSTR (SQLERRM, 1, 230));
                END;
            END IF;
        ELSE                                        -- if p_is_partitioned =1;
            --loop1
            FOR t IN (SELECT DISTINCT partition_name, truncated
                        FROM user_tab_modifications m
                       WHERE m.table_name = UPPER (p_table_name))
            LOOP
                p_stale_flag := 0;

                IF (t.truncated = 'YES')
                THEN
                    p_stale_flag := 1;
                ELSE
                    IF (t.partition_name IS NOT NULL)
                    THEN
                        SELECT num_rows, last_analyzed
                          INTO p_num_rows, p_last_analyzed
                          FROM user_tab_partitions
                         WHERE     table_name = UPPER (p_table_name)
                               AND partition_name = t.partition_name;

                        SELECT (CASE
                                    WHEN (  SUM (
                                                  m.inserts
                                                + m.updates
                                                + m.deletes)
                                          -   p_num_rows
                                            * p_change_percent
                                            / 100) >=
                                         0
                                    THEN
                                        1
                                    ELSE
                                        0
                                END)
                                   flag
                          INTO p_stale_flag
                          FROM user_tab_modifications m
                         WHERE     m.table_name = UPPER (p_table_name)
                               AND m.partition_name = t.partition_name
                               AND m.timestamp >= p_last_analyzed;
                    ELSE
                        SELECT num_rows, last_analyzed
                          INTO p_num_rows, p_last_analyzed
                          FROM user_tables
                         WHERE table_name = UPPER (p_table_name);

                        SELECT (CASE
                                    WHEN (  SUM (
                                                  m.inserts
                                                + m.updates
                                                + m.deletes)
                                          - p_num_rows * 0.2) >=
                                         0
                                    THEN
                                        1
                                    ELSE
                                        0
                                END)
                                   flag
                          INTO p_stale_flag
                          FROM user_tab_modifications m
                         WHERE     m.table_name = UPPER (p_table_name)
                               AND m.timestamp >= p_last_analyzed;
                    END IF;
                END IF;

                IF (p_stale_flag >= 1)
                THEN
                    BEGIN
                        pro_put_line (
                               'start to gather statistics on table  '
                            || p_table_name
                            || ' partition '
                            || t.partition_name
                            || '  (begin)');

                        IF (t.partition_name IS NOT NULL)
                        THEN
                            DBMS_STATS.GATHER_TABLE_STATS (
                                ownname       => p_user,
                                tabname       => p_table_name,
                                partname      => t.partition_name,
                                --estimate_percent => p_estimate_percent,--commented by Rajesh J for fine tuning on 9-Oct-2012 for CR C02251989
                                estimate_percent   =>
                                    DBMS_STATS.AUTO_SAMPLE_SIZE, --added by Rajesh J for fine tuning on 9-Oct-2012 for CR C02251989
                                --method_opt       => p_method_opt,--commented by Rajesh J for fine tuning on 17-Oct-2012 for CR C02251989
                                --method_opt       => 'FOR ALL COLUMNS SIZE 1', --added by Rajesh J for fine tuning on 17-Oct-2012 for CR C02251989
                                method_opt    => 'FOR ALL COLUMNS SIZE AUTO', --added by Rajesh J for fine tuning on 25-Oct-2012 for CR C02251989
                                degree        => V_DEGREE, --added by Rajesh J for fine tuning on 4-Oct-2012 for CR C02251989--Added by DXC for performance fix Jira#OP-2195
                                granularity   => 'PARTITION',
                                CASCADE       => TRUE);
                        ELSE
                            DBMS_STATS.GATHER_TABLE_STATS (
                                ownname       => p_user,
                                tabname       => p_table_name,
                                partname      => t.partition_name,
                                --estimate_percent => p_estimate_percent, --commented by Rajesh J for fine tuning on 9-Oct-2012 for CR C02251989
                                estimate_percent   =>
                                    DBMS_STATS.AUTO_SAMPLE_SIZE, --added by Rajesh J for fine tuning on 9-Oct-2012 for CR C02251989
                                --method_opt       => p_method_opt,--commented by Rajesh J for fine tuning on 17-Oct-2012 for CR C02251989
                                --method_opt       => 'FOR ALL COLUMNS SIZE 1', --added by Rajesh J for fine tuning on 17-Oct-2012 for CR C02251989
                                method_opt    => 'FOR ALL COLUMNS SIZE AUTO', --added by Rajesh J for fine tuning on 25-Oct-2012 for CR C02251989
                                degree        => V_DEGREE, --added by Rajesh J for fine tuning on 4-Oct-2012 for CR C02251989--Added by DXC for performance fix Jira#OP-2195
                                granularity   => 'GLOBAL',
                                CASCADE       => TRUE);
                        END IF;

                        pro_put_line (
                               'start to gather statistics on table  '
                            || p_table_name
                            || ' partition '
                            || t.partition_name
                            || ' (end-ok)');
                    EXCEPTION
                        WHEN unusable_index
                        THEN
                            BEGIN
                                pro_put_line (
                                       'warning: meet unusable_index on table  '
                                    || p_table_name
                                    || '  partition '
                                    || t.partition_name
                                    || ', rebuild it');
                                opt_rebuild_ind (p_table_name,
                                                 NULL,
                                                 'N',
                                                 'Y');
                                pro_put_line (
                                       'start to gather statistics on table  '
                                    || p_table_name
                                    || ' partition '
                                    || t.partition_name
                                    || '(begin)');
                                DBMS_STATS.GATHER_TABLE_STATS (
                                    ownname       => p_user,
                                    tabname       => p_table_name,
                                    partname      => t.partition_name,
                                    --estimate_percent => p_estimate_percent,--commented by Rajesh J for fine tuning on 9-Oct-2012 for CR C02251989
                                    estimate_percent   =>
                                        DBMS_STATS.AUTO_SAMPLE_SIZE, --added by Rajesh J for fine tuning on 9-Oct-2012 for CR C02251989
                                    --method_opt       => p_method_opt,--commented by Rajesh J for fine tuning on 17-Oct-2012 for CR C02251989
                                    --method_opt       => 'FOR ALL COLUMNS SIZE 1', --added by Rajesh J for fine tuning on 17-Oct-2012 for CR C02251989
                                    method_opt    =>
                                        'FOR ALL COLUMNS SIZE AUTO', --added by Rajesh J for fine tuning on 25-Oct-2012 for CR C02251989
                                    degree        => V_DEGREE, --added by Rajesh J for fine tuning on 4-Oct-2012 for CR C02251989 --Added by DXC for performance fix Jira#OP-2195
                                    granularity   => 'AUTO',
                                    CASCADE       => TRUE);
                                pro_put_line (
                                       'start to gather statistics on table  '
                                    || p_table_name
                                    || ' partition '
                                    || t.partition_name
                                    || '(end-ok)');
                            EXCEPTION
                                WHEN OTHERS
                                THEN
                                    RAISE_APPLICATION_ERROR (
                                        c_gather_tbl_stats_error,
                                           ' Error at gather table stats'
                                        || SUBSTR (SQLERRM, 1, 230));
                            END;
                        WHEN OTHERS
                        THEN
                            RAISE_APPLICATION_ERROR (
                                c_gather_tbl_stats_error,
                                   ' Error at gather table stats'
                                || SUBSTR (SQLERRM, 1, 230));
                    END;
                END IF;
            END LOOP;
        END IF;
    END dyn_tab_stats;

    /*
   ***************************************************************************
   * Program : opt_unuable_ind
   * Version : 1.0
   * Author  : Myra (huifang.cao@hp.com)
   * Date    : 20-May-2009
   * Purpose : unusable indexes
   ***************************************************************************
  */
    PROCEDURE opt_unusable_ind (p_Table_Name       VARCHAR2,
                                skip_unique_flag   VARCHAR2 DEFAULT 'N')
    IS
    BEGIN
        FOR m IN (SELECT index_name, uniqueness
                    FROM user_indexes
                   WHERE table_name = p_Table_Name)
        LOOP
            BEGIN
                -- Modified to skip unique index if skip_unique_flag is 'Y'
                -- Simon 16:28 2009-9-28
                IF skip_unique_flag = 'Y'
                THEN
                    IF m.uniqueness = 'UNIQUE'
                    THEN
                        pro_put_line (
                               'Skip unique index '
                            || m.index_name
                            || ' on table '
                            || p_Table_Name
                            || ' as skip_unique_flag is Y');
                    ELSE
                        pro_put_line (
                               'Begin to unusable unique index '
                            || m.index_name
                            || ' on table '
                            || p_Table_Name);

                        EXECUTE IMMEDIATE   'alter index '
                                         || m.index_name
                                         || ' unusable ';
                    END IF;
                ELSE
                    pro_put_line (
                           'Begin to unusable index '
                        || m.index_name
                        || ' on table '
                        || p_Table_Name);

                    EXECUTE IMMEDIATE   'alter index '
                                     || m.index_name
                                     || ' unusable ';
                END IF;
            EXCEPTION
                WHEN OTHERS
                THEN
                    RAISE_APPLICATION_ERROR (
                        c_build_ind_error,
                           ' Error at unusable '
                        || m.index_name
                        || ' on table '
                        || p_Table_Name
                        || SUBSTR (SQLERRM, 1, 230));
            END;
        END LOOP;
    END;

    /*
    ***************************************************************************
    * Program : opt_rebuild_ind
    * Version : 1.0
    * Author  : Myra (huifang.cao@hp.com)
    * Date    : 13-Jun-2007
    * Purpose : rebuild unusable indexes on table/partition/subpartition
    * Parameters :
    *              p_table_name
    * Process:                                              --
    * 1.0      if is_unusable = 'Y', has unusable indexes, rebuild them on table/partition/subpartition
    *          if is_unusable = 'N', default value, always rebuld indexes on table/partition/sub/partition
    * 3.1.     rebuild unusable normal indices on this table/partition.
    * 3.2.     rebuild unusable partitioned indices on this table/partition.
    * Change History
    * Date         Programmer         Description
    * -------------------------------------------------------------------------
    * 30-OCT-2005   Myra              Initial Version
    * 08-Nov-2010   Leo               Optima11, fix Oracle optimizer issue in 11g upgrade
    ****************************************************************************
    */
    PROCEDURE opt_rebuild_ind (p_Table_Name         VARCHAR2,
                               p_Index_Name         VARCHAR2 DEFAULT NULL,
                               exclude_bitmap_ind   VARCHAR2 DEFAULT 'N',
                               is_unusable          VARCHAR2 DEFAULT 'N',
                               cmprs_ind            VARCHAR2 DEFAULT 'N',
                               ind_tablespace       VARCHAR2 DEFAULT NULL)
    IS
        p_is_partitioned       NUMBER := 0;
        p_is_subpartitioned    NUMBER := 0;
        p_count                NUMBER := 0;
        p_rebuld_flg           NUMBER := 0;
        p_index_flg            NUMBER := 0;  -- has parameter index_name input
        p_ind_tablespace       VARCHAR2 (30);
        p_exclude_bitmap_ind   NUMBER := 0;
        P_cmprs_ind            VARCHAR2 (30);
        v_cmprs_ind            VARCHAR2 (30);
    BEGIN
        --step 1.1 rebuild unusable normal indices on this table/partition.
        pro_put_line (
            'Checking indexes on opt table ' || p_Table_Name || ' (begin)');

        pro_put_line ('Checking partition/subpartition (begin)');

        IF ((is_unusable = 'N') OR (is_unusable IS NULL))
        THEN
            p_rebuld_flg := 1;
        END IF;

        IF (   (cmprs_ind = 'N')
            OR (cmprs_ind IS NULL)
            OR (p_Index_Name IS NOT NULL))
        THEN
            p_cmprs_ind := '';
        ELSE
            IF (cmprs_ind = 'Y')
            THEN
                p_cmprs_ind := ' COMPRESS ';
            END IF;
        END IF;

        IF (exclude_bitmap_ind = 'N') OR (exclude_bitmap_ind IS NULL)
        THEN
            p_exclude_bitmap_ind := 0;
        ELSE
            IF (exclude_bitmap_ind = 'Y')
            THEN
                p_exclude_bitmap_ind := 1;
            END IF;
        END IF;

        BEGIN
            IF (p_Index_Name IS NULL)
            THEN
                SELECT DECODE (partitioned, 'YES', 1, 0)
                  INTO p_is_partitioned
                  FROM user_tables
                 WHERE table_name = UPPER (p_Table_Name);
            ELSE
                p_index_flg := 1;

                SELECT DECODE (partitioned, 'YES', 1, 0),
                       DECODE (uniqueness,
                               'UNIQUE', '',
                               DECODE (partitioned, 'YES', '', p_cmprs_ind))
                           AS cmprs_ind,
                       DECODE (ind_tablespace,
                               NULL, tablespace_name,
                               ind_tablespace)
                           ind_tablespace
                  INTO p_is_partitioned, v_cmprs_ind, p_ind_tablespace
                  FROM user_indexes
                 WHERE     table_name = UPPER (p_Table_Name)
                       AND index_name = p_Index_Name;
            END IF;
        EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
                pro_put_line (
                       'Error: table '
                    || p_Table_Name
                    || ' or index '
                    || p_Index_Name
                    || ' doesn''t exist, please check!');
                raise_application_error (
                    c_no_jobname_error,
                    ' Error: table or index doesn''t exist, please check!');
        END;

        -- check whether the table is subpartitioned.
        SELECT COUNT (*)
          INTO p_count
          FROM user_tab_subpartitions
         WHERE table_name = UPPER (p_Table_Name);

        IF (p_count > 0)
        THEN
            p_is_subpartitioned := 1;
        END IF;

        IF ((p_index_flg = 1) AND (p_is_partitioned = 0))
        THEN
            pro_put_line (
                   'Begin to rebulid index '
                || p_Table_Name
                || '.'
                || p_Index_Name
                || ' on tablespace '
                || p_ind_tablespace
                || v_cmprs_ind
                || '.');

            BEGIN
                EXECUTE IMMEDIATE   'alter index '
                                 || p_Index_Name
                                 || ' rebuild '
                                 || ' tablespace '
                                 || p_ind_tablespace
                                 || ' PARALLEL 2 '
                                 || v_cmprs_ind;

                pro_put_line (
                       'End to rebulid index '
                    || p_Table_Name
                    || '.'
                    || p_Index_Name
                    || '.');
            EXCEPTION
                WHEN OTHERS
                THEN
                    RAISE_APPLICATION_ERROR (
                        c_build_ind_error,
                           ' Error at rebuilding  normal indexes:'
                        || SUBSTR (SQLERRM, 1, 230));
            END;
        ELSE
            FOR rec
                IN (SELECT DISTINCT
                           index_name,
                           DECODE (
                               uniqueness,
                               'UNIQUE', '',
                               DECODE (partitioned, 'YES', '', p_cmprs_ind))
                               AS cmprs_ind,
                           DECODE (ind_tablespace,
                                   NULL, tablespace_name,
                                   ind_tablespace)
                               ind_tablespace
                      FROM user_indexes
                     WHERE     (status = 'UNUSABLE' OR p_rebuld_flg = 1)
                           AND (   index_type <> 'BITMAP'
                                OR p_exclude_bitmap_ind = 0)
                           AND table_name = UPPER (p_Table_Name)
                           AND partitioned = 'NO')
            LOOP
                BEGIN
                    pro_put_line (
                           'Begin to rebulid '
                        || p_Table_Name
                        || '.'
                        || rec.index_name
                        || ' on tablespace '
                        || rec.ind_tablespace
                        || '.');

                    EXECUTE IMMEDIATE   'alter index '
                                     || rec.index_name
                                     || ' rebuild'
                                     || ' tablespace '
                                     || rec.ind_tablespace
                                     || ' PARALLEL 2 ';

                    pro_put_line (
                           'End to rebulid '
                        || p_Table_Name
                        || '.'
                        || rec.index_name
                        || '.');
                EXCEPTION
                    WHEN OTHERS
                    THEN
                        RAISE_APPLICATION_ERROR (
                            c_build_ind_error,
                               ' Error at rebuilding  normal indexes:'
                            || SUBSTR (SQLERRM, 1, 230));
                END;
            END LOOP;
        END IF;

        --step 3.2
        -- if it's table with local partition indexes, check if any partitioned indexes unusable on the table, if has rebuild them

        --step 4
        IF ((P_IS_PARTITIONED = 1) AND (P_IS_SUBPARTITIONED = 0))
        THEN
            IF (P_INDEX_FLG = 1)
            THEN
                BEGIN
                    FOR REC1
                        IN (SELECT DISTINCT
                                   IP.PARTITION_NAME,
                                   DECODE (
                                       I.UNIQUENESS,
                                       'UNIQUE', '',
                                       DECODE (I.PARTITIONED,
                                               'YES', '',
                                               P_CMPRS_IND))
                                       AS CMPRS_IND,
                                   DECODE (IND_TABLESPACE,
                                           NULL, IP.TABLESPACE_NAME,
                                           IND_TABLESPACE)
                                       IND_TABLESPACE
                              FROM USER_IND_PARTITIONS  IP,
                                   USER_INDEXES         I,
                                   USER_TAB_PARTITIONS  P
                             WHERE     (   IP.STATUS = 'UNUSABLE'
                                        OR P_REBULD_FLG = 1)
                                   AND (   I.INDEX_TYPE <> 'BITMAP'
                                        OR P_EXCLUDE_BITMAP_IND = 0)
                                   AND IP.INDEX_NAME = I.INDEX_NAME
                                   AND I.TABLE_NAME = P.TABLE_NAME
                                   AND P.TABLE_NAME = UPPER (P_TABLE_NAME))
                    LOOP
                        BEGIN
                            PRO_PUT_LINE (
                                   'Begin to rebulid index '
                                || P_TABLE_NAME
                                || '.'
                                || P_INDEX_NAME
                                || ' on partition '
                                || REC1.PARTITION_NAME
                                || ' on tablespace '
                                || REC1.IND_TABLESPACE);

                            EXECUTE IMMEDIATE   'alter index '
                                             || P_INDEX_NAME
                                             || ' rebuild partition '
                                             || REC1.PARTITION_NAME
                                             || ' tablespace '
                                             || REC1.IND_TABLESPACE
                                             || ' PARALLEL 2 ';

                            PRO_PUT_LINE (
                                   'End to rebulid index '
                                || P_TABLE_NAME
                                || '.'
                                || P_INDEX_NAME
                                || ' on partition '
                                || REC1.PARTITION_NAME
                                || ' on tablespace '
                                || REC1.IND_TABLESPACE);
                        EXCEPTION
                            WHEN OTHERS
                            THEN
                                RAISE_APPLICATION_ERROR (
                                    C_BUILD_IND_ERROR,
                                       ' Error at rebuilding  partitioned indexes:'
                                    || SUBSTR (SQLERRM, 1, 230));
                        END;
                    END LOOP;
                END;
            ELSE
                FOR REC
                    IN (SELECT DISTINCT
                               I.INDEX_NAME,
                               DECODE (
                                   I.UNIQUENESS,
                                   'UNIQUE', '',
                                   DECODE (I.PARTITIONED,
                                           'YES', '',
                                           P_CMPRS_IND))
                                   AS CMPRS_IND,
                               DECODE (IND_TABLESPACE,
                                       NULL, IP.TABLESPACE_NAME,
                                       IND_TABLESPACE)
                                   IND_TABLESPACE
                          FROM USER_IND_PARTITIONS IP, USER_INDEXES I
                         WHERE     (   IP.STATUS = 'UNUSABLE'
                                    OR P_REBULD_FLG = 1)
                               AND (   I.INDEX_TYPE <> 'BITMAP'
                                    OR P_EXCLUDE_BITMAP_IND = 0)
                               AND IP.INDEX_NAME = I.INDEX_NAME
                               AND I.TABLE_NAME = UPPER (P_TABLE_NAME))
                LOOP
                    BEGIN
                        FOR REC1
                            IN (SELECT DISTINCT IP.PARTITION_NAME --p.partition_name
                                  FROM USER_IND_PARTITIONS  IP,
                                       USER_INDEXES         I,
                                       USER_TABLES          P --Optima11, Oracle optimizer issue in 11g upgrade, 20101108, Leo
                                 WHERE     (   IP.STATUS = 'UNUSABLE'
                                            OR P_REBULD_FLG = 1)
                                       AND (   I.INDEX_TYPE <> 'BITMAP'
                                            OR P_EXCLUDE_BITMAP_IND = 0)
                                       AND IP.INDEX_NAME = I.INDEX_NAME
                                       AND I.TABLE_NAME = P.TABLE_NAME
                                       AND P.PARTITIONED = 'YES' -- Optima11, Oracle optimizer issue in 11g upgrade, 20101108, Leo
                                       AND P.TABLE_NAME =
                                           UPPER (P_TABLE_NAME))
                        LOOP
                            BEGIN
                                PRO_PUT_LINE (
                                       'Begin to rebulid index '
                                    || P_TABLE_NAME
                                    || '.'
                                    || REC.INDEX_NAME
                                    || ' on partition '
                                    || REC1.PARTITION_NAME
                                    || ' on tablespace '
                                    || REC.IND_TABLESPACE);

                                EXECUTE IMMEDIATE   'alter index '
                                                 || REC.INDEX_NAME
                                                 || ' rebuild partition '
                                                 || REC1.PARTITION_NAME
                                                 || ' tablespace '
                                                 || REC.IND_TABLESPACE
                                                 || ' PARALLEL 2 ';

                                PRO_PUT_LINE (
                                       'Begin to rebulid index '
                                    || P_TABLE_NAME
                                    || '.'
                                    || REC.INDEX_NAME
                                    || ' on partition '
                                    || REC1.PARTITION_NAME
                                    || ' on tablespace '
                                    || REC.IND_TABLESPACE);
                            EXCEPTION
                                WHEN OTHERS
                                THEN
                                    RAISE_APPLICATION_ERROR (
                                        C_BUILD_IND_ERROR,
                                           ' Error at rebuilding  partitioned indexes:'
                                        || SUBSTR (SQLERRM, 1, 230));
                            END;
                        END LOOP;
                    END;
                END LOOP;
            END IF;
        ELSE
            IF ((P_IS_PARTITIONED = 1) AND (P_IS_SUBPARTITIONED = 1))
            THEN
                IF (P_INDEX_FLG = 1)
                THEN
                    BEGIN
                        FOR REC4
                            IN (SELECT DISTINCT
                                       IP.SUBPARTITION_NAME,
                                       DECODE (
                                           I.UNIQUENESS,
                                           'UNIQUE', '',
                                           DECODE (I.PARTITIONED,
                                                   'YES', '',
                                                   P_CMPRS_IND))
                                           AS CMPRS_IND,
                                       DECODE (IND_TABLESPACE,
                                               NULL, IP.TABLESPACE_NAME,
                                               IND_TABLESPACE)
                                           IND_TABLESPACE
                                  FROM USER_IND_SUBPARTITIONS  IP,
                                       USER_INDEXES            I,
                                       USER_TAB_SUBPARTITIONS  P
                                 WHERE     (   IP.STATUS = 'UNUSABLE'
                                            OR P_REBULD_FLG = 1)
                                       AND (   I.INDEX_TYPE <> 'BITMAP'
                                            OR P_EXCLUDE_BITMAP_IND = 0)
                                       AND IP.INDEX_NAME = I.INDEX_NAME
                                       AND I.TABLE_NAME = P.TABLE_NAME
                                       AND P.TABLE_NAME =
                                           UPPER (P_TABLE_NAME))
                        LOOP
                            BEGIN
                                PRO_PUT_LINE (
                                       'Begin to rebulid index '
                                    || P_TABLE_NAME
                                    || '.'
                                    || P_INDEX_NAME
                                    || ' on subpartition '
                                    || REC4.SUBPARTITION_NAME
                                    || ' on tablespace '
                                    || REC4.IND_TABLESPACE);

                                EXECUTE IMMEDIATE   'alter index '
                                                 || P_INDEX_NAME
                                                 || ' rebuild subpartition '
                                                 || REC4.SUBPARTITION_NAME
                                                 || ' tablespace '
                                                 || REC4.IND_TABLESPACE
                                                 || ' PARALLEL 2 ';

                                PRO_PUT_LINE (
                                       'End to rebulid index '
                                    || P_TABLE_NAME
                                    || '.'
                                    || P_INDEX_NAME
                                    || ' on subpartition '
                                    || REC4.SUBPARTITION_NAME
                                    || ' on tablespace '
                                    || REC4.IND_TABLESPACE);
                            EXCEPTION
                                WHEN OTHERS
                                THEN
                                    RAISE_APPLICATION_ERROR (
                                        C_BUILD_IND_ERROR,
                                           ' Error at rebuilding  subpartitioned indexes:'
                                        || SUBSTR (SQLERRM, 1, 230));
                            END;
                        END LOOP;
                    END;
                ELSE
                    FOR REC3
                        IN (SELECT DISTINCT
                                   I.INDEX_NAME,
                                   DECODE (
                                       UNIQUENESS,
                                       'UNIQUE', '',
                                       DECODE (I.PARTITIONED,
                                               'YES', '',
                                               P_CMPRS_IND))
                                       AS CMPRS_IND,
                                   DECODE (IND_TABLESPACE,
                                           NULL, IP.TABLESPACE_NAME,
                                           IND_TABLESPACE)
                                       IND_TABLESPACE
                              FROM USER_IND_SUBPARTITIONS IP, USER_INDEXES I
                             WHERE     (   IP.STATUS = 'UNUSABLE'
                                        OR P_REBULD_FLG = 1)
                                   AND (   I.INDEX_TYPE <> 'BITMAP'
                                        OR P_EXCLUDE_BITMAP_IND = 0)
                                   AND IP.INDEX_NAME = I.INDEX_NAME
                                   AND I.TABLE_NAME = UPPER (P_TABLE_NAME))
                    LOOP
                        BEGIN
                            FOR REC4
                                IN (SELECT DISTINCT IP.SUBPARTITION_NAME
                                      FROM USER_IND_SUBPARTITIONS  IP,
                                           USER_INDEXES            I,
                                           USER_TAB_SUBPARTITIONS  P
                                     WHERE     (   IP.STATUS = 'UNUSABLE'
                                                OR P_REBULD_FLG = 1)
                                           AND (   I.INDEX_TYPE <> 'BITMAP'
                                                OR P_EXCLUDE_BITMAP_IND = 0)
                                           AND IP.INDEX_NAME = I.INDEX_NAME
                                           AND I.TABLE_NAME = P.TABLE_NAME
                                           AND P.TABLE_NAME =
                                               UPPER (P_TABLE_NAME))
                            LOOP
                                BEGIN
                                    PRO_PUT_LINE (
                                           'Begin to rebulid index '
                                        || P_TABLE_NAME
                                        || '.'
                                        || REC3.INDEX_NAME
                                        || ' on subpartition '
                                        || REC4.SUBPARTITION_NAME
                                        || ' on tablespace '
                                        || REC3.IND_TABLESPACE);

                                    EXECUTE IMMEDIATE   'alter index '
                                                     || REC3.INDEX_NAME
                                                     || ' rebuild subpartition '
                                                     || REC4.SUBPARTITION_NAME
                                                     || ' tablespace '
                                                     || P_IND_TABLESPACE
                                                     || REC3.IND_TABLESPACE
                                                     || '  PARALLEL 2 ';

                                    PRO_PUT_LINE (
                                           'End to rebulid index '
                                        || P_TABLE_NAME
                                        || '.'
                                        || REC3.INDEX_NAME
                                        || ' on subpartition '
                                        || REC4.SUBPARTITION_NAME
                                        || ' on tablespace '
                                        || REC3.IND_TABLESPACE);
                                EXCEPTION
                                    WHEN OTHERS
                                    THEN
                                        RAISE_APPLICATION_ERROR (
                                            C_BUILD_IND_ERROR,
                                               ' Error at rebuilding  subpartitioned indexes:'
                                            || SUBSTR (SQLERRM, 1, 230));
                                END;
                            END LOOP;
                        END;
                    END LOOP;
                END IF;

                PRO_PUT_LINE ('Checking unusable indexes on opt table (end)');
            END IF;
        END IF;

        PRO_PUT_LINE (
            'rebuld indexes on opt table ' || P_TABLE_NAME || ' (end)');
    END OPT_REBUILD_IND;


    PROCEDURE optima_stats_ptbl (p_table_name   VARCHAR2,
                                 prttn_name     VARCHAR2,
                                 degree_cnt     NUMBER DEFAULT NULL)
    IS
        /*
        ***************************************************************************
        * Program : optima_stats_ptbl
        * Version : 1.0
        * Author  : Daniel (hong-jun.qin@hp.com)
        * Date    : 24-Aug-2011
        * Purpose : collect given partition of table p_Table_Name statistics
        * Parameters : p_table_name  table name
        *              prttn_name    partition name
        *              degree_cnt    use to parallel gather stats
        * Process:
        * 1.   get some information e.g. user...
        * 2.   use dbms_stats.gather_table_stats to gather special partition's stats
        * Change History
        * Date         Programmer         Description
        * -------------------------------------------------------------------------
        * 24-Aug-2011  Daniel.Qin         Initial Version
        ****************************************************************************
        */
        p_user               VARCHAR2 (30);
		V_SQL                VARCHAR2 (32767);
        p_stats_methd_optn   OPT_TBL_MTDTA_PRC.STATS_METHD_OPTN%TYPE
                                 := 'FOR ALL COLUMNS SIZE AUTO';
        p_stats_estmt_pct    OPT_TBL_MTDTA_PRC.STATS_ESTMT_PCT%TYPE := 10;

        v_stats_methd_optn   OPT_TBL_MTDTA_PRC.STATS_METHD_OPTN%TYPE;
        v_stats_estmt_pct    OPT_TBL_MTDTA_PRC.STATS_ESTMT_PCT%TYPE;
    BEGIN
        --step 1
        --get user of tables, and parameters for gathering stats
        SELECT username
          INTO p_user
          FROM user_users
         WHERE ROWNUM = 1;

        pro_put_line (
               'Checking parameters used when gathering stats for prttn '
            || prttn_name
            || ' (begin)');

        BEGIN
              SELECT MAX (stats_methd_optn), MAX (stats_estmt_pct)
                INTO v_stats_methd_optn, v_stats_estmt_pct
                FROM opt_tbl_mtdta_prc
               WHERE UPPER (tbl_name) = UPPER (p_table_name)
            GROUP BY tbl_name;
        EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
                pro_put_line (
                       'Wrong: table '
                    || p_table_name
                    || ' doesn''t exist in OPT_TBL_MTDTA_PRC, default parameters will be used!');
                v_stats_methd_optn := p_stats_methd_optn;
                v_stats_estmt_pct := p_stats_estmt_pct;
        END;

        pro_put_line (
               'Checking parameters used when gathering stats for prttn '
            || prttn_name
            || ' (End)');

        pro_put_line (
            'gather statistics for prttn ' || prttn_name || '(begin)');

        IF prttn_name IS NOT NULL
        THEN
            BEGIN
                DBMS_STATS.gather_table_stats (
                    ownname            => p_user,
                    tabname            => p_table_name,
                    partname           => prttn_name,
                    --estimate_percent   => v_stats_estmt_pct,--commented by Rajesh J for fine tuning on 9-Oct-2012 for CR C02251989
                    estimate_percent   => DBMS_STATS.AUTO_SAMPLE_SIZE, --added by Rajesh J for fine tuning on 9-Oct-2012 for CR C02251989
                    --method_opt         => v_stats_methd_optn,--commented by Rajesh J for fine tuning on 17-Oct-2012 for CR C02251989
                    --method_opt       => 'FOR ALL COLUMNS SIZE 1', --added by Rajesh J for fine tuning on 17-Oct-2012 for CR C02251989
                    method_opt         => 'FOR ALL COLUMNS SIZE AUTO', --added by Rajesh J for fine tuning on 25-Oct-2012 for CR C02251989
                    granularity        => 'AUTO',
                    --degree             => degree_cnt, --commented by Rajesh J for fine tuning on 4-Oct-2012 for CR C02251989
                    degree             => 16, --added by Rajesh J for fine tuning on 4-Oct-2012 for CR C02251989
                    cascade            => FALSE);
            EXCEPTION
                WHEN unusable_index
                THEN
			BEGIN
                        pro_put_line (
                            'warning: meet unusable_index on this table, rebuild it');
                        opt_rebuild_ind (p_table_name,
                                         NULL,
                                         'N',
                                         'Y');
                        pro_put_line (
                               'start to gather statistics for prttn '
                            || prttn_name
                            || ' (begin)');
                        DBMS_STATS.gather_table_stats (
                            ownname            => p_user,
                            tabname            => p_table_name,
                            partname           => prttn_name,
                            --estimate_percent   => v_stats_estmt_pct,--commented by Rajesh J for fine tuning on 9-Oct-2012 for CR C02251989
                            estimate_percent   => DBMS_STATS.AUTO_SAMPLE_SIZE, --added by Rajesh J for fine tuning on 9-Oct-2012 for CR C02251989
                            --method_opt         => v_stats_methd_optn,--commented by Rajesh J for fine tuning on 17-Oct-2012 for CR C02251989
                            --method_opt       => 'FOR ALL COLUMNS SIZE 1', --added by Rajesh J for fine tuning on 17-Oct-2012 for CR C02251989
                            method_opt         => 'FOR ALL COLUMNS SIZE AUTO', --added by Rajesh J for fine tuning on 25-Oct-2012 for CR C02251989
                            granularity        => 'AUTO',
                            --degree             => degree_cnt, --commented by Rajesh J for fine tuning on 4-Oct-2012 for CR C02251989
                            degree             => 16, --added by Rajesh J for fine tuning on 4-Oct-2012 for CR C02251989
                            cascade            => FALSE);
                        pro_put_line (
                               'start to gather statistics for prttn '
                            || prttn_name
                            || ' (end-ok)');
                    EXCEPTION
                        WHEN OTHERS
                        THEN
                            raise_application_error (
                                c_gather_tbl_stats_error,
                                   ' Error at gather table stats'
                                || SUBSTR (SQLERRM, 1, 230));
                    END;
                WHEN OTHERS
                THEN
                    raise_application_error (
                        c_gather_tbl_stats_error,
                           ' Error at gather table stats'
                        || SUBSTR (SQLERRM, 1, 230));
            END;
        END IF;

        pro_put_line (
            'gather statistics for prttn ' || prttn_name || ' (end)');
    END optima_stats_ptbl;
END opt_gather_stats;
/
