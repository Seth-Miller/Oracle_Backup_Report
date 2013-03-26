CREATE OR REPLACE PROCEDURE backup_html_email (
   database            IN VARCHAR2 DEFAULT NULL,
   emailaddresses      IN VARCHAR2,
   inputtext           IN VARCHAR2 DEFAULT NULL,
   excludermanoutput   IN BOOLEAN DEFAULT FALSE,
   includelinktosp     IN BOOLEAN DEFAULT FALSE)
IS
   v_from           VARCHAR2 (80) := 'New_Backup_Server';
   v_mail_host      VARCHAR2 (30) := '';
   v_subject        VARCHAR2 (80) := 'New Prod Backup Report';
   report_link      VARCHAR2 (150)
      := '';
   server_timezone  varchar2(50) := 'GMT';
   status_link      NUMBER;
   l_database       VARCHAR2 (500) := database;
   l_addresses      VARCHAR2 (1000) := emailaddresses;
   l_inputtext      VARCHAR2 (900) := inputtext;
   l_exclude        BOOLEAN := excludermanoutput;
   l_includesp      BOOLEAN := includelinktosp;
   l_count          NUMBER;
   l_array          DBMS_UTILITY.lname_array;
   dbid             NUMBER;
   inst_id          NUMBER;
   bid              NUMBER;
   eid              NUMBER;
   db_unique_name   VARCHAR2 (30);
   host_name        VARCHAR2 (64);
   starttime        CHAR (5);
   endtime          CHAR (5);
   v_mail_conn      UTL_SMTP.connection;
   lineoutput       VARCHAR2 (2000);
   output1          VARCHAR2 (2000);
   output2          VARCHAR2 (2000);
   output3          VARCHAR2 (2000);
   output2_1        VARCHAR2 (2000);
   output2_2        VARCHAR2 (2000);
BEGIN
   DECLARE
      /*
      Cursor c1_rec builds the list of databases to be checked for backup.
      This list will include all databases in the repository even if the
      backup was not successful. If a database was passed to this function
      it will be the only database that shows up in the report, otherwise
      all of the databases will show.

      Cursor c2_rec will pull the actual rman session output for the entire
      backup session for each database being checked and add it to the report.
      If the database being checked does not have a backup recoreded in the
      repository, the rman session output section for that database
      will be skipped.
      */
      CURSOR c1_rec
      IS
           SELECT session_key, operation, dbid, db_name
             FROM bkupadm.last_backup
            WHERE     operation = 'BACKUP SESSION'
                  AND NVL2 (l_database, db_name, 1) =
                         NVL (UPPER (l_database), 1)
         ORDER BY db_name;

      CURSOR c2_rec
      IS
         SELECT DISTINCT session_key, db_name, dbid
           FROM    rman.rc_rman_output
                JOIN
                   bkupadm.last_backup
                USING (session_key)
          WHERE     operation = 'BACKUP SESSION'
                AND NVL2 (l_database, db_name, 1) =
                       NVL (UPPER (l_database), 1)
                AND EXISTS
                       (SELECT 1
                          FROM    bkupadm.last_backup_details
                               JOIN
                                  bkupadm.last_backup
                               USING (session_key)
                         WHERE status <> 'MISSING');
   BEGIN
      IF l_exclude = FALSE
      THEN
         status_link := 0;
      END IF;


      /*
      check_backups is a separate function that builds the last_backup table that
      the c1_rec cursor will pull data from. It contains the latest backup and latest
      block corruption check sessions pulled from the rman catalog repository.
      It needs to be run everytime so this function has the latest data to work with.
      */
      bkupadm.check_backups;

      -- This array will be used to create an array of the comma delimited email
      -- addresses passed to this function.
      DBMS_UTILITY.comma_to_table (list     => l_addresses,
                                   tablen   => l_count,
                                   tab      => l_array);

      -- Open the SMTP connection
      v_mail_conn := UTL_SMTP.OPEN_CONNECTION (v_mail_host, 25);
      -- Perform handshake after connection is open
      UTL_SMTP.HELO (v_mail_conn, v_mail_host);
      -- Initiate a mail transaction with the mail server
      UTL_SMTP.MAIL (v_mail_conn, v_from);

      -- Build the recipient list from the comma delimited emails passed to the function
      FOR i IN 1 .. l_count
      LOOP
         UTL_SMTP.RCPT (v_mail_conn, REPLACE (l_array (i), '"', ''));
      END LOOP;

      -- Send the DATA command indicating the following lines are the body of the email
      UTL_SMTP.OPEN_DATA (v_mail_conn);

      -- Each WRITE_DATA line writes a line to the body of the email
      -- From: will show up as the From: in the email
      UTL_SMTP.WRITE_DATA (v_mail_conn, 'From:' || v_from || UTL_TCP.CRLF);

      -- Subject line of the email
      UTL_SMTP.WRITE_DATA (
         v_mail_conn,
         'Subject: ' || v_subject || UTL_TCP.CRLF);

      -- The email format is plain html to make it highly compatible
      UTL_SMTP.WRITE_DATA (
         v_mail_conn,
            'Content-Type: text/html; charset=utf8'
         || UTL_TCP.CRLF
         || UTL_TCP.CRLF);

      -- If text is passed to the function, it will show up at the top of the email
      -- before any of the report
      UTL_SMTP.WRITE_DATA (
         v_mail_conn,
         '<p>' || l_inputtext || '</p>' || UTL_TCP.CRLF || UTL_TCP.CRLF);

      IF l_includesp = TRUE
      THEN
         UTL_SMTP.WRITE_DATA (
            v_mail_conn,
               '<p>'
            || '<a href="'
            || report_link
            || '">Go to Full Report</a>'
            || '</p>'
            || UTL_TCP.CRLF
            || UTL_TCP.CRLF);
      END IF;

      /*
      This is the beginning of the master table. It will contain a header row and a row
      for each database. The first and last cell of each row will span three rows in order
      to separate the backup report from the block corruption report and eventually the
      datapump export report.
      */
      lineoutput := '<table border=1 align="left">';
      UTL_SMTP.WRITE_DATA (v_mail_conn, lineoutput || UTL_TCP.CRLF);

      -- Header line of the table
      lineoutput :=
         '<tr><th width=60>Database Name/Server</th><th>Operation</th>';
      UTL_SMTP.WRITE_DATA (v_mail_conn, lineoutput || UTL_TCP.CRLF);
      lineoutput :=
         '<th>Start Time</th><th>End Time</th><th>Elapsed</th><th>Status</th>';
      UTL_SMTP.WRITE_DATA (v_mail_conn, lineoutput || UTL_TCP.CRLF);
      lineoutput := '<th>Backup Breakdown</th></tr>';
      UTL_SMTP.WRITE_DATA (v_mail_conn, lineoutput || UTL_TCP.CRLF);


      /*
      For each loop, a database from the c1_rec cursor is checked and printed. There are
      multiple output variables because html puts exclamation points in funny places if
      lines code are too long. Each output variable is built such that the limit is not
      met resulting in exclamation point free code.
      */
      FOR i IN c1_rec
      LOOP
         BEGIN
            -- The first cell spans three rows and contains the database name and the server
            -- or cluster name in which it resides. The second cell spans only one row and is
            -- labeled as backup.
            BEGIN
            SELECT    '<tr><td rowspan=3>'
                   || database_name
                   || '<p>'
                   || server
                   || '</p></td><td>BACKUP</td>'
              INTO output1
              FROM bkupadm.database_inventory
             WHERE dbid = i.dbid;
            EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               output1 :=    '<tr><td rowspan=3 bgcolor="yellow">'
                   || 'Update <p> Inventory for <p>'
                   || i.db_name || '<p>'
                   || to_char(i.dbid)
                   || '</p></td><td>BACKUP</td>';
            END;

            /*
            The third cell is the start time of the backup session. If the backup started
            two days ago, the cell will be yellow. If the backup started three days ago or
            earlier the cell will be red. If the start time is null then the cell will be
            blank and white. The cell will otherwise be green indicating the backup started
            sometime today or yesterday.
            */
            SELECT    '<td '
                   || CASE
                         WHEN TRUNC (start_time) = TRUNC (server_time - 2)
                         THEN
                            'bgcolor="yellow"> '
                         WHEN TRUNC (start_time) < TRUNC (server_time - 3)
                         THEN
                            'bgcolor="red"> '
                         WHEN start_time IS NULL
                         THEN
                            'bgcolor="white"> '
                         ELSE
                            'bgcolor="lime"> '
                      END
                   || TO_CHAR (start_time, 'MM-DD-YY HH:MI PM')
                   || '</td><td '
                   /*
                   The fourth cell is the end time of the backup session. If the backup ended
                   two days ago, the cell will be yellow. If the backup ended three days ago or
                   earlier the cell will be red. If the end time is null then the cell will be
                   blank and white. The cell will otherwise be green indicating the backup ended
                   sometime today or yesterday.
                   */
                   || CASE
                         WHEN TRUNC (end_time) = TRUNC (server_time - 2)
                         THEN
                            'bgcolor="yellow"> '
                         WHEN TRUNC (end_time) < TRUNC (server_time - 3)
                         THEN
                            'bgcolor="red"> '
                         WHEN end_time IS NULL
                         THEN
                            'bgcolor="white"> '
                         ELSE
                            'bgcolor="lime"> '
                      END
                   || CASE
                         WHEN status = 'RUNNING' THEN ''
                         ELSE TO_CHAR (end_time, 'MM-DD-YY HH:MI PM')
                      END
                   || '</td><td '
                   /*
                   The fifth cell is the amount of time the backup took in hours and minutes
                   separated by a colon.

                   If the backup took less than an our the cell will be green. If the backup took
                   at least an hour but less than two, the cell will be yellow. If the backup
                   took two hours or more, the cell will be red. If the backup time is null,
                   the cell will be blank and white. The cell will otherwise be green.
                   */
                   || CASE
                         WHEN FLOOR ( (end_time - start_time) * 24) = 0
                         THEN
                            'bgcolor="lime"> '
                         WHEN FLOOR ( (end_time - start_time) * 24) = 1
                         THEN
                            'bgcolor="yellow"> '
                         WHEN FLOOR ( (end_time - start_time) * 24) > 1
                         THEN
                            'bgcolor="red"> '
                         WHEN end_time IS NULL
                         THEN
                            'bgcolor="white"> '
                         ELSE
                            'bgcolor="lime"> '
                      END
                   /*
                   FM00 is a variable for to_char to prevent blank spaces in front of the number.
                   The first number is the total number of hours that the backup took.
                   The floor function is used to strip the decimal from the product of 24 and
                   the end time less the start time. The actual number of hours is printed as the
                   first number in the cell.
                   */
                   || TO_CHAR (FLOOR ( (end_time - start_time) * 24), 'FM00')
                   -- A colon separates the hours and minutes of the total backup time but will
                   -- not be printed if the end time is null.
                   || NVL2 (end_time, ':', NULL)
                   /*
                   FM00 is a variable for to_char to prevent blank spaces in front of the number.
                   The second number is the total number of minutes in addition to the hours
                   that the backup took. The floor function is used to strip the decimal from the
                   product of 24 and the end time less the start time multiplied by 60. Mod then
                   divides that number by 60 and returns the remainder. The actual number of
                   minutes is printed as the second number in the cell following the colon.
                   */
                   || TO_CHAR (
                         MOD (FLOOR ( (end_time - start_time) * 24 * 60), 60),
                         'FM00')
                   || '</td><td '
                   /*
                   The sixth cell is the status of the backup according to the parent row of the
                   backup session in the repository. The value will be one of COMPLETED, RUNNING,
                   MISSING or null.
                   */
                   || CASE
                         WHEN status = 'COMPLETED'
                         THEN
                            'bgcolor="lime">'
                         WHEN SUBSTR (status, -6) = 'ERRORS'
                         THEN
                            'bgcolor="red">'
                         WHEN SUBSTR (status, -8) = 'WARNINGS'
                         THEN
                            'bgcolor="yellow">'
                         WHEN status = 'RUNNING'
                         THEN
                            'bgcolor="gold">'
                         WHEN status = 'MISSING'
                         THEN
                            'bgcolor="moccasin">'
                         ELSE
                            '>'
                      END
                   || CASE
                         WHEN status_link = 0
                         THEN
                               '<a href="#'
                            || session_key
                            || '">'
                            || status
                            || '</a>'
                         ELSE
                            status
                      END
                   || '</td>'
              INTO output2
              FROM    bkupadm.last_backup
                   JOIN
                      (SELECT session_key,
                              operation,
                              status,
                              CASE (SELECT local_timezone
                                      FROM bkupadm.database_inventory
                                     WHERE dbid = l.dbid)
                                 WHEN 'US/Central'
                                 THEN
                                    start_time
                                 ELSE
                                    CAST (
                                       FROM_TZ (
                                          CAST (start_time AS TIMESTAMP),
                                          NVL ( (SELECT local_timezone
                                                   FROM bkupadm.database_inventory
                                                  WHERE dbid = l.dbid),
                                               'GMT'))
                                          AT TIME ZONE 'US/Central' AS DATE)
                              END
                                 start_time,
                              CASE (SELECT local_timezone
                                      FROM bkupadm.database_inventory
                                     WHERE dbid = l.dbid)
                                 WHEN 'US/Central'
                                 THEN
                                    end_time
                                 ELSE
                                    CAST (
                                       FROM_TZ (CAST (end_time AS TIMESTAMP),
                                                NVL ( (SELECT local_timezone
                                                         FROM bkupadm.database_inventory
                                                        WHERE dbid = l.dbid),
                                                     'GMT'))
                                          AT TIME ZONE 'US/Central' AS DATE)
                              END
                                 end_time,
                                 CASE server_timezone
                                 WHEN 'US/Central'
                                 THEN
                                    SYSDATE
                                 ELSE
                                    CAST (
                                       SYSTIMESTAMP
                                          AT TIME ZONE 'US/Central' AS DATE)
                              END                                 
                                 server_time
                         FROM    bkupadm.last_backup_details lbd
                              JOIN
                                 bkupadm.last_backup l
                              USING (session_key, operation))
                   USING (session_key, operation)
             WHERE session_key = i.session_key AND operation = i.operation;
         EXCEPTION
            WHEN OTHERS
            THEN
               RAISE_APPLICATION_ERROR (
                  -20000,
                  'An error has occurred brother: ' || SQLERRM);
               UTL_SMTP.CLOSE_DATA (v_mail_conn);
               UTL_SMTP.QUIT (v_mail_conn);
         END;

         BEGIN
                SELECT    '<td rowspan=3><table border=0 align="left">'
                       || '<font size="1">'
                       || REPLACE (SYS_CONNECT_BY_PATH (operation, ','), ',')
                       || '</font></table></td></tr>'
                  INTO output3
                  FROM (SELECT session_key,
                                  '<tr><td width=200>'
                               || LTRIM (operation)
                               || '</td><td>'
                               || status
                               || '</td></tr>'
                                  AS operation,
                               ROW_NUMBER () OVER (ORDER BY start_time) - 1
                                  AS seq
                          FROM bkupadm.last_backup_details
                         WHERE session_key = (SELECT i.session_key FROM DUAL))
                 WHERE CONNECT_BY_ISLEAF = 1
            CONNECT BY seq = PRIOR seq + 1
            START WITH seq = 1;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               output3 := '<td></td></tr>';
         END;

         SELECT '<tr><td>BLKCHK</td>' INTO output2_1 FROM DUAL;

         BEGIN
            SELECT    '<td '
                   || CASE
                         WHEN TRUNC (start_time) = TRUNC (server_time - 2)
                         THEN
                            'bgcolor="yellow"> '
                         WHEN TRUNC (start_time) < TRUNC (server_time - 3)
                         THEN
                            'bgcolor="red"> '
                         WHEN start_time IS NULL
                         THEN
                            'bgcolor="white"> '
                         ELSE
                            'bgcolor="lime"> '
                      END
                   || TO_CHAR (start_time, 'MM-DD-YY HH:MI PM')
                   || '</td><td '
                   || CASE
                         WHEN TRUNC (end_time) = TRUNC (server_time - 2)
                         THEN
                            'bgcolor="yellow"> '
                         WHEN TRUNC (end_time) < TRUNC (server_time - 3)
                         THEN
                            'bgcolor="red"> '
                         WHEN end_time IS NULL
                         THEN
                            'bgcolor="white"> '
                         ELSE
                            'bgcolor="lime"> '
                      END
                   || CASE
                         WHEN status = 'RUNNING' THEN ''
                         ELSE TO_CHAR (end_time, 'MM-DD-YY HH:MI PM')
                      END
                   || '</td><td '
                   || CASE
                         WHEN FLOOR ( (end_time - start_time) * 24) = 0
                         THEN
                            'bgcolor="lime"> '
                         WHEN FLOOR ( (end_time - start_time) * 24) = 1
                         THEN
                            'bgcolor="yellow"> '
                         WHEN FLOOR ( (end_time - start_time) * 24) > 1
                         THEN
                            'bgcolor="red"> '
                         WHEN end_time IS NULL
                         THEN
                            'bgcolor="white"> '
                         ELSE
                            'bgcolor="lime"> '
                      END
                   || TO_CHAR (FLOOR ( (end_time - start_time) * 24), 'FM00')
                   || NVL2 (end_time, ':', NULL)
                   || TO_CHAR (
                         MOD (FLOOR ( (end_time - start_time) * 24 * 60), 60),
                         'FM00')
                   || '</td><td '
                   || CASE
                         WHEN status = 'COMPLETED'
                         THEN
                            'bgcolor="lime">'
                         WHEN SUBSTR (status, -6) = 'ERRORS'
                         THEN
                            'bgcolor="red">'
                         WHEN SUBSTR (status, -8) = 'WARNINGS'
                         THEN
                            'bgcolor="yellow">'
                         WHEN status = 'RUNNING'
                         THEN
                            'bgcolor="gold">'
                         WHEN status = 'MISSING'
                         THEN
                            'bgcolor="moccasin">'
                         ELSE
                            '>'
                      END
                   || status
                   || '</td></tr>'
              INTO output2_2
              FROM    bkupadm.last_backup
                   JOIN
                      (SELECT session_key,
                              operation,
                              status,
                              CASE (SELECT local_timezone
                                      FROM bkupadm.database_inventory
                                     WHERE dbid = l.dbid)
                                 WHEN 'US/Central'
                                 THEN
                                    start_time
                                 ELSE
                                    CAST (
                                       FROM_TZ (
                                          CAST (start_time AS TIMESTAMP),
                                          NVL ( (SELECT local_timezone
                                                   FROM bkupadm.database_inventory
                                                  WHERE dbid = l.dbid),
                                               'GMT'))
                                          AT TIME ZONE 'US/Central' AS DATE)
                              END
                                 start_time,
                              CASE (SELECT local_timezone
                                      FROM bkupadm.database_inventory
                                     WHERE dbid = l.dbid)
                                 WHEN 'US/Central'
                                 THEN
                                    end_time
                                 ELSE
                                    CAST (
                                       FROM_TZ (CAST (end_time AS TIMESTAMP),
                                                NVL ( (SELECT local_timezone
                                                         FROM bkupadm.database_inventory
                                                        WHERE dbid = l.dbid),
                                                     'GMT'))
                                          AT TIME ZONE 'US/Central' AS DATE)
                              END
                                 end_time,
                                 CASE server_timezone
                                 WHEN 'US/Central'
                                 THEN
                                    SYSDATE
                                 ELSE
                                    CAST (
                                       SYSTIMESTAMP
                                          AT TIME ZONE 'US/Central' AS DATE)
                              END                                 
                                 server_time
                         FROM    bkupadm.last_backup_details lbd
                              JOIN
                                 bkupadm.last_backup l
                              USING (session_key, operation))
                   USING (session_key, operation)
             WHERE dbid = i.dbid AND operation = 'BACKUP VALIDATE SESSION';
         EXCEPTION
            WHEN OTHERS
            THEN
               RAISE_APPLICATION_ERROR (
                  -20000,
                  'An error has occurred brother: ' || SQLERRM);
               UTL_SMTP.CLOSE_DATA (v_mail_conn);
               UTL_SMTP.QUIT (v_mail_conn);
         END;

         UTL_SMTP.WRITE_DATA (v_mail_conn, output1 || UTL_TCP.CRLF);
         UTL_SMTP.WRITE_DATA (v_mail_conn,
                              SUBSTR (output2, 1, 990) || UTL_TCP.CRLF);
         UTL_SMTP.WRITE_DATA (v_mail_conn,
                              SUBSTR (output2, 991) || UTL_TCP.CRLF);
         UTL_SMTP.WRITE_DATA (v_mail_conn, output3 || UTL_TCP.CRLF);
         UTL_SMTP.WRITE_DATA (v_mail_conn, output2_1 || UTL_TCP.CRLF);
         UTL_SMTP.WRITE_DATA (v_mail_conn,
                              SUBSTR (output2_2, 1, 990) || UTL_TCP.CRLF);
         UTL_SMTP.WRITE_DATA (v_mail_conn,
                              SUBSTR (output2_2, 991) || UTL_TCP.CRLF);
         UTL_SMTP.WRITE_DATA (
            v_mail_conn,
               '<tr><td></td><td></td><td></td><td></td><td></td></tr>'
            || UTL_TCP.CRLF);
      END LOOP;


      lineoutput := '</table><p><br clear="left">';
      UTL_SMTP.WRITE_DATA (v_mail_conn, lineoutput || UTL_TCP.CRLF);

      IF l_includesp = TRUE
      THEN
         UTL_SMTP.WRITE_DATA (
            v_mail_conn,
               '<p>'
            || '<a href="'
            || report_link
            || '">Go to Full Report</a>'
            || '</p>'
            || UTL_TCP.CRLF
            || UTL_TCP.CRLF);
      END IF;


      IF l_exclude = FALSE
      THEN
         FOR i IN c2_rec
         LOOP
            lineoutput := '<table border=1 align="left">';
            UTL_SMTP.WRITE_DATA (v_mail_conn, lineoutput || UTL_TCP.CRLF);
            lineoutput :=
                  '<tr><th bgcolor="#D8D8D8"><a name="'
               || i.session_key
               || '">'
               || i.db_name
               || ' ('
               || i.dbid
               || ') '
               || 'Output</a></th></tr>';
            UTL_SMTP.WRITE_DATA (v_mail_conn, lineoutput || UTL_TCP.CRLF);

            FOR c2_subrec
               IN (  SELECT    '<tr><td bgcolor="#F0F0F0"'
                            || DECODE (output, ' ', ' height="20"')
                            || '>'
                            || output
                            || '</td></tr>'
                               output
                       FROM rman.rc_rman_output
                      WHERE session_key = i.session_key
                   ORDER BY recid)
            LOOP
               UTL_SMTP.WRITE_DATA (v_mail_conn,
                                    c2_subrec.output || UTL_TCP.CRLF);
            END LOOP;

            lineoutput := '</table><p><br clear="left">';
            UTL_SMTP.WRITE_DATA (v_mail_conn, lineoutput || UTL_TCP.CRLF);
         END LOOP;
      END IF;

      UTL_SMTP.CLOSE_DATA (v_mail_conn);
      UTL_SMTP.QUIT (v_mail_conn);
   EXCEPTION
      WHEN UTL_SMTP.TRANSIENT_ERROR OR UTL_SMTP.PERMANENT_ERROR
      THEN
         RAISE_APPLICATION_ERROR (-20000, 'Unable to send mail: ' || SQLERRM);
   END;
END backup_html_email;
/
