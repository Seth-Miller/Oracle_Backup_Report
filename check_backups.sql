CREATE OR REPLACE PROCEDURE check_backups
AS
   TYPE b1 IS RECORD
   (
      database_name   database_inventory.database_name%TYPE,
      dbid            database_inventory.dbid%TYPE
   );

   TYPE databases_to_check_type IS TABLE OF b1;

   --cursor c1 is select session_key, db_name, db_key, start_time, status, operation from rman.rc_rman_status;
   --type rc_rman_stat_type is table of c1%rowtype;
   TYPE c1 IS RECORD
   (
      session_key   rman.rc_rman_status.session_key%TYPE,
      db_name       rman.rc_rman_status.db_name%TYPE,
      db_key        rman.rc_rman_status.db_key%TYPE,
      start_time    rman.rc_rman_status.start_time%TYPE,
      end_time      rman.rc_rman_status.end_time%TYPE,
      status        rman.rc_rman_status.status%TYPE,
      object_type   rman.rc_rman_status.object_type%TYPE,
      operation     VARCHAR2 (100)
   );

   TYPE rc_rman_stat_type IS TABLE OF c1;

   databases_to_check   databases_to_check_type;
   rc_rman_stat         rc_rman_stat_type;
BEGIN
   /*   SELECT database_name, dbid
        BULK COLLECT INTO databases_to_check
        FROM database_inventory;
   */
   SELECT name, dbid
     BULK COLLECT INTO databases_to_check
     FROM rman.rc_database;

   DELETE last_backup;

   DELETE last_backup_details;

   FOR q IN 1 .. databases_to_check.COUNT
   LOOP
      BEGIN
             SELECT session_key,
                    db_name,
                    db_key,
                    start_time,
                    end_time,
                    status,
                    object_type,
                    CASE LEVEL
                       WHEN 1
                       THEN
                          CASE operation
                             WHEN 'RMAN' THEN 'BACKUP SESSION'
                          END
                       ELSE
                          LPAD (' ', LEVEL * 3) || operation
                    END
               BULK COLLECT INTO rc_rman_stat
               FROM rman.rc_rman_status
              WHERE session_key =
                       (SELECT DISTINCT session_key
                          FROM rman.rc_rman_status r,
                               (  SELECT db_key, MAX (start_time) start_time
                                    FROM rman.rc_rman_status
                                   WHERE db_key =
                                            (SELECT db_key
                                               FROM rman.rc_database
                                              WHERE dbid =
                                                       databases_to_check (q).dbid)
                               and operation = 'BACKUP'
                               GROUP BY db_key) f
                         WHERE     r.start_time = f.start_time
                               AND r.db_key = f.db_key)
                               START WITH parent_key IS NULL
         CONNECT BY PRIOR rsr_key = parent_key;

         IF rc_rman_stat.COUNT = 0
         THEN
            INSERT INTO last_backup (session_key, operation, db_name, dbid)
                 VALUES (
                              1||databases_to_check (q).dbid
                           || databases_to_check (q).dbid,
                           'BACKUP SESSION',
                           databases_to_check (q).database_name,
                           databases_to_check (q).dbid);

            INSERT INTO last_backup_details (session_key, operation, status)
                 VALUES (
                              1||databases_to_check (q).dbid
                           || databases_to_check (q).dbid,
                           'BACKUP SESSION',
                           'MISSING');
         ELSE
            INSERT INTO last_backup (session_key,
                                     operation,
                                     db_name,
                                     db_key,
                                     dbid)
                 VALUES (rc_rman_stat (1).session_key,
                         'BACKUP SESSION',
                         rc_rman_stat (1).db_name,
                         rc_rman_stat (1).db_key,
                         databases_to_check (q).dbid);

            FORALL i IN 1 .. rc_rman_stat.COUNT
               INSERT INTO last_backup_details (session_key,
                                                start_time,
                                                end_time,
                                                operation,
                                                status,
                                                object_type)
                    VALUES (rc_rman_stat (i).session_key,
                            rc_rman_stat (i).start_time,
                            rc_rman_stat (i).end_time,
                            rc_rman_stat (i).operation,
                            rc_rman_stat (i).status,
                            rc_rman_stat (i).object_type);
         END IF;
      EXCEPTION
            WHEN OTHERS
            THEN
               RAISE_APPLICATION_ERROR (
                  -20000,
                  'An error has occurred in the first select statement on dbid '||databases_to_check (q).dbid||': ' || SQLERRM);
      END;
      BEGIN
      rc_rman_stat := null;
             SELECT session_key,
                    db_name,
                    db_key,
                    start_time,
                    end_time,
                    status,
                    object_type,
                    CASE LEVEL
                       WHEN 1
                       THEN
                          CASE operation
                             WHEN 'RMAN' THEN 'BACKUP VALIDATE SESSION'
                          END
                       ELSE
                          LPAD (' ', LEVEL * 3) || operation
                    END
               BULK COLLECT INTO rc_rman_stat
               FROM rman.rc_rman_status
              WHERE session_key =
                       (SELECT DISTINCT session_key
                          FROM rman.rc_rman_status r,
                               (  SELECT db_key, MAX (start_time) start_time
                                    FROM rman.rc_rman_status
                                   WHERE db_key =
                                            (SELECT db_key
                                               FROM rman.rc_database
                                              WHERE dbid =
                                                       databases_to_check (q).dbid)
                               and operation = 'BACKUP VALIDATE'
                               GROUP BY db_key) f
                         WHERE     r.start_time = f.start_time
                               AND r.db_key = f.db_key)
                               START WITH parent_key IS NULL
         CONNECT BY PRIOR rsr_key = parent_key;

         IF rc_rman_stat.COUNT = 0
         THEN
            INSERT INTO last_backup (session_key, operation, db_name, dbid)
                 VALUES (
                              2||databases_to_check (q).dbid
                           || databases_to_check (q).dbid,
                           'BACKUP VALIDATE SESSION',
                           databases_to_check (q).database_name,
                           databases_to_check (q).dbid);

            INSERT INTO last_backup_details (session_key, operation, status)
                 VALUES (
                              2||databases_to_check (q).dbid
                           || databases_to_check (q).dbid,
                           'BACKUP VALIDATE SESSION',
                           'MISSING');
         ELSE
            INSERT INTO last_backup (session_key,
                                     operation,
                                     db_name,
                                     db_key,
                                     dbid)
                 VALUES (rc_rman_stat (1).session_key,
                         'BACKUP VALIDATE SESSION',
                         rc_rman_stat (1).db_name,
                         rc_rman_stat (1).db_key,
                         databases_to_check (q).dbid);

            FORALL i IN 1 .. rc_rman_stat.COUNT
               INSERT INTO last_backup_details (session_key,
                                                start_time,
                                                end_time,
                                                operation,
                                                status,
                                                object_type)
                    VALUES (rc_rman_stat (i).session_key,
                            rc_rman_stat (i).start_time,
                            rc_rman_stat (i).end_time,
                            rc_rman_stat (i).operation,
                            rc_rman_stat (i).status,
                            rc_rman_stat (i).object_type);
         END IF;
      EXCEPTION WHEN OTHERS
            THEN
               RAISE_APPLICATION_ERROR (
                  -20000,
                  'An error has occurred in the second select statement on dbid '||databases_to_check (q).dbid||': ' || SQLERRM);
      END;
   END LOOP;

   COMMIT;
END check_backups;
/
