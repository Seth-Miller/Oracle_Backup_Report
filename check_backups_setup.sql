# Create <BACKUP_ADMIN_SCHEMA> schema




# Create directory for file drop

CREATE DIRECTORY REPORT_DROP AS '<MAIN_DIRECTORY>';
GRANT READ, WRITE ON DIRECTORY REPORT_DROP TO <BACKUP_ADMIN_SCHEMA>;




# Create database inventory table

CREATE TABLE <BACKUP_ADMIN_SCHEMA>.DATABASE_INVENTORY (DATABASE_NAME VARCHAR2(10), DBID NUMBER, SERVER VARCHAR2(50), LOCAL_TIMEZONE VARCHAR2(20), DPEXPORTFILTER VARCHAR2(30), DPEXPORTVALUE VARCHAR2(255));




# Populate database inventory table with default data

INSERT INTO <BACKUP_ADMIN_SCHEMA>.DATABASE_INVENTORY (DATABASE_NAME, DBID) SELECT DISTINCT NAME, DBID FROM RMAN.RC_DATABASE ORDER BY 1;




# Manually populate the rest of the fields




# Create last backup tables

CREATE TABLE <BACKUP_ADMIN_SCHEMA>.LAST_BACKUP (SESSION_KEY NUMBER, OPERATION VARCHAR2(30), DB_NAME VARCHAR2(8), DB_KEY NUMBER, DBID NUMBER, JOB_STATE VARCHAR2(50));

CREATE TABLE <BACKUP_ADMIN_SCHEMA>.LAST_BACKUP_DETAILS (SESSION_KEY NUMBER, START_TIME DATE, END_TIME DATE, OPERATION VARCHAR2(100), OPERATION_TYPE VARCHAR2(25), STATUS VARCHAR2(33), OBJECT_TYPE VARCHAR2(25));




# Give grants to rman tables to backup user

GRANT SELECT ON RMAN.RC_DATABASE TO <BACKUP_ADMIN_SCHEMA>;
GRANT SELECT ON RMAN.RC_RMAN_STATUS TO <BACKUP_ADMIN_SCHEMA>;
GRANT SELECT ON RMAN.RC_RMAN_OUTPUT TO <BACKUP_ADMIN_SCHEMA>;




# Create ACLs for the email package

BEGIN 
DBMS_NETWORK_ACL_ADMIN.CREATE_ACL (  
ACL          => 'mail_access.xml',  
DESCRIPTION  => 'Permissions to access e-mail server.',  
PRINCIPAL    => 'PUBLIC',  
IS_GRANT     => TRUE,  
PRIVILEGE    => 'connect');  
COMMIT;  
END;  

BEGIN 
DBMS_NETWORK_ACL_ADMIN.ASSIGN_ACL (  
ACL          => 'mail_access.xml',  
HOST         => '<MAIL_HOST>',  
LOWER_PORT   => 25,  
UPPER_PORT   => 25  
);  
COMMIT;  
END; 

COLUMN host FORMAT A30
COLUMN acl FORMAT A30

SELECT host, lower_port, upper_port, acl
FROM   dba_network_acls;




# Create check_backups stored procedure

@check_backups.sql




# Execute check backups to test

EXECUTE <BACKUP_ADMIN_SCHEMA>.CHECK_BACKUPS;




# Query the new backup tables for details

         SELECT DISTINCT SESSION_KEY, DB_NAME, DBID
           FROM    RMAN.RC_RMAN_OUTPUT
                JOIN
                   <BACKUP_ADMIN_SCHEMA>.LAST_BACKUP
                USING (SESSION_KEY)
          WHERE     OPERATION = 'BACKUP SESSION'
                AND EXISTS
                       (SELECT 1
                          FROM    <BACKUP_ADMIN_SCHEMA>.LAST_BACKUP_DETAILS
                               JOIN
                                  <BACKUP_ADMIN_SCHEMA>.LAST_BACKUP
                               USING (SESSION_KEY)
                         WHERE STATUS <> 'MISSING');
                         
                         

		
# Create backup_html_email procedure

@backup_html_email.sql




# Create backup_html_file procedure

@backup_html_file.sql




# Create the host_command_file_transfer procedure

@host_command_file_transfer.sql




/*
# Execute backup_html_email procedure

EXEC <BACKUP_ADMIN_SCHEMA>.backup_html_email(EMAILADDRESSES => '"<email addresses>"', EXCLUDERMANOUTPUT => TRUE, INCLUDELINKTOSP => FALSE);




# Fire job from script (use this for one-offs)

if [ <test statement> ]; then sqlplus <BACKUP_ADMIN_SCHEMA>/<password>@rmanctp << EOF
begin
<BACKUP_ADMIN_SCHEMA>.backup_html_email(
emailaddresses => '"<email addresses>"'
,database => '<DBID>'
);
end;
/
exit
EOF
fi
*/




# Set up cron to execute the OS side command to push the full html file to sharepoint (optional)

*/1 * * * * export XDIR=<Server Directory>; [ "$( cat $XDIR/finishflagfile )" = "1" ] && ( echo 0 > $XDIR/finishflagfile && $XDIR/hostexecfile.sh ) > /dev/null 2>&1




# Create SMB credentials file in the <Server Directory> called svccreds




# Run the whole process manually for testing

BEGIN
-- Run this for the full file saved to the file system
<BACKUP_ADMIN_SCHEMA>.BACKUP_HTML_FILE(P_DIR => 'REPORT_DROP', P_FILENAME => 'html_backup_report_prod.htm (change to preference)');
-- Run this to move the file from the local file system to the sharepoint site
<BACKUP_ADMIN_SCHEMA>.HOST_COMMAND_FILE_TRANSFER(
   L_DIR => 'REPORT_DROP'
  ,L_FILENAME => 'hostexecfile.sh'
  ,F_FILENAME => 'finishflagfile'
  ,FILE_DIR => '<Server Directory (same as $XDIR in crontab entry)>'
  ,REPORT_FILE => 'html_backup_report_prod.htm (change to preference)'
  ,CREDS_FILE => 'svccreds'
  ,REMOTE_SHARE => '<Remote Share Parent Directory (i.e. //fileserver/depts)>'
  ,REMOTE_DIR => '<Remote Share File Directory (i.e. database)>');
-- Run this to generated the trimmed down email report
<BACKUP_ADMIN_SCHEMA>.backup_html_email(EMAILADDRESSES => '"<email addresses>"', INPUTTEXT => 'This is a test of the truncated backup report.', EXCLUDERMANOUTPUT => TRUE, INCLUDELINKTOSP => TRUE);
END;
/




# Create a scheduled job to run the report

BEGIN
DBMS_SCHEDULER.CREATE_JOB(
JOB_NAME => '<BACKUP_ADMIN_SCHEMA>."Daily Backup Report"',
JOB_TYPE => 'PLSQL_BLOCK',
JOB_ACTION => '
BEGIN

<BACKUP_ADMIN_SCHEMA>.BACKUP_HTML_FILE(P_DIR => ''REPORT_DROP'', P_FILENAME => ''html_backup_report_prod.htm (change to preference)'');
<BACKUP_ADMIN_SCHEMA>.HOST_COMMAND_FILE_TRANSFER(
   L_DIR => ''REPORT_DROP''
  ,L_FILENAME => ''hostexecfile.sh''
  ,F_FILENAME => ''finishflagfile''
  ,FILE_DIR => ''<Server Directory (same as $XDIR in crontab entry)>''
  ,REPORT_FILE => ''html_backup_report_prod.htm (change to preference)''
  ,CREDS_FILE => ''svccreds''
  ,REMOTE_SHARE => ''<Remote Share Parent Directory (i.e. //fileserver/depts)>''
  ,REMOTE_DIR => ''<Remote Share File Directory (i.e. database)>'');
<BACKUP_ADMIN_SCHEMA>.backup_html_email(EMAILADDRESSES => ''"<email addresses>"'', EXCLUDERMANOUTPUT => TRUE, INCLUDELINKTOSP => TRUE);

END;',
START_DATE => to_timestamp_tz('09/13/2012 09:00 AM -5', 'MM/DD/YYYY HH:MI AM TZH'),
--START_DATE => systimestamp,
--START_DATE => '27-JUL-12 9.00.00AM US/Central',
REPEAT_INTERVAL => 'FREQ=DAILY',
ENABLED => TRUE);
END;
