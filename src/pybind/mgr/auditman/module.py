import json
import time
import errno
import logging
import sqlite3
from collections import deque
from dataclasses import dataclass, astuple
from threading import Thread, Lock, Condition
from typing import List, Optional

from .cli import AuditManCLICommand

from mgr_module import MgrModule, Option, NotifyType

log = logging.getLogger(__name__)


@dataclass(frozen=True)  # 'frozen' makes it immutable and thread-safe
class AuditEntry:
    timestamp: str
    user: str
    user_host: str
    entity_name: str
    command: str
    args: str
    state: str
    retval: int
    priority: int
    sequence: int
    epoch: int
    from_name: str
    from_rank: int
    result_message: str = ""

    def to_sqlite_tuple(self) -> tuple:
        """
        Returns the data in the exact order required by the
        INSERT statement.
        """
        return astuple(self)

    @classmethod
    def from_native(cls, log_entry):
        """
        Factory method to create an AuditEntry from a regex match object
        and the original raw message dictionary.
        """
        logmsg = log_entry.logmsg
        if logmsg.cmd:
            return cls(
                timestamp=log_entry.stamp,
                user=logmsg.name,
                user_host=logmsg.addrs,
                entity_name=logmsg.entity_name,
                command=logmsg.cmd,
                args=logmsg.cmd_args,
                state=logmsg.cmd_state,
                retval=logmsg.cmd_retval,
                priority=log_entry.prio,
                sequence=log_entry.seq,
                epoch=log_entry.epoch,
                from_name=log_entry.name,
                from_rank=log_entry.rank,
                result_message=''
            )
        else:
            # stash the entire msg as command
            return cls(
                timestamp=log_entry.stamp,
                user=logmsg.name,
                user_host='',
                entity_name='',
                command=log_entry.msg,
                args='',
                state='',
                retval=0,
                priority=log_entry.prio,
                sequence=log_entry.seq,
                epoch=log_entry.epoch,
                from_name=log_entry.name,
                from_rank=log_entry.rank,
                result_message=''
            )


class Module(MgrModule):
    CLICommand = AuditManCLICommand
    MODULE_OPTIONS: List[Option] = []
    NOTIFY_TYPES = [NotifyType.audit]
    MON_AUDIT_CMD_NS = '.mon.ns'

    MODULE_OPTIONS = [
        {
            "name": "retention_days",
            "type": "int",
            "default": 30,
            "desc": "Maximum age of audit records in days",
            "runtime": True,
        },
        {
            "name": "max_records",
            "type": "int",
            "default": 1000000,
            "desc": "Maximum number of records to keep in the database",
            "runtime": True,
        },
    ]

    def serve(self):
        last_prune = 0
        PRUNE_INTERVAL = 3600  # Run once an hour
        while not self.stopping:
            now = time.time()
            if now - last_prune > PRUNE_INTERVAL:
                self.apply_retention_policy()
                last_prune = now
            time.sleep(60)  # Wake up every minute to check for shutdown

    def apply_retention_policy(self):
        """Applies both time-based and count-based retention policies."""
        days = self.get_module_option("retention_days")
        max_rows = self.get_module_option("max_records")

        log.debug(f"Applying retention: {days} days / {max_rows} rows")
        try:
            with self.conn_lock:
                cursor = self.dbconn.cursor()

                # Identify the ID we must protect
                cursor.execute("SELECT MAX(rowid) FROM audit_commands")
                last_id_row = cursor.fetchone()
                if not last_id_row or last_id_row[0] is None:
                    return

                protected_id = last_id_row[0]

                # 1. Time-based Pruning
                # Deletes anything older than the retention window
                retention_interval = f"{days} days"
                cursor.execute(
                    """
                    DELETE FROM audit_commands
                    WHERE datetime(timestamp) < datetime('now', '-' || ?)
                    AND rowid != ?
                    """, (retention_interval, protected_id))
                time_pruned = cursor.rowcount

                # 2. Row-count Pruning
                # If we are still over the limit, delete the oldest rows
                # by rowid
                cursor.execute(
                    """
                    DELETE FROM audit_commands
                    WHERE rowid NOT IN (
                    SELECT rowid FROM audit_commands
                    ORDER BY epoch DESC, sequence DESC
                    LIMIT ?) AND rowid != ?
                    """, (max_rows, protected_id))
                count_pruned = cursor.rowcount
                self.dbconn.commit()

            if time_pruned > 0 or count_pruned > 0:
                log.info(f"Retention applied: {time_pruned} removed by age, "
                         f"{count_pruned} by count.")
        except Exception as e:
            log.error(f"Retention policy failed: {e}")

    @AuditManCLICommand.Read('audit fetch')
    def audit_fetch(self,
                    limit: Optional[int] = 100,
                    entity: Optional[str] = None):
        return self.show_audit_records(limit, entity)

    def show_audit_records(self, limit, entity):
        query = """
        SELECT timestamp, user, user_host, entity_name, command,
        args, retval, sequence, epoch, state FROM audit_commands """
        params = []

        if entity:
            query += "WHERE entity_name LIKE ? "
            params.append(f"%{entity}%")
        query += "ORDER BY epoch DESC, sequence DESC LIMIT ?"
        params.append(limit)
        try:
            with self.conn_lock:
                cursor = self.dbconn.cursor()
                cursor.execute(query, params)
                rows = cursor.fetchall()
                results = [
                    {
                        "timestamp": r[0],
                        "user": r[1],
                        "user_host": r[2],
                        "entity_name": r[3],
                        "command": r[4],
                        "args": r[5],
                        "retval": r[6],
                        "state": r[9]
                    } for r in rows
                ]
                return (0, json.dumps(results, indent=2), "")
        except Exception as e:
            return (-errno.EINVAL, "", f"Failed to query audit database: {e}")

    def notify(self, notify_type: NotifyType, log_entry):
        log.debug(f'notify_type={notify_type}')
        log.debug(f'logmsg={log_entry.logmsg}')
        if not notify_type == 'audit':
            return
        entry = AuditEntry.from_native(log_entry)
        with self.qlock:
            self.q.append(entry.to_sqlite_tuple())
            self.qcond.notify_all()

    def audit_recorder(self):
        INSERT_AUDIT = '''INSERT INTO audit_commands(
            timestamp, user, user_host, entity_name, command, args,
            state, retval, priority,
            sequence, epoch, from_name, from_rank, result_message)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);'''
        try:
            self.qlock.acquire()
            while not self.stopping:
                while not len(self.q):
                    log.debug('audit queue empty')
                    self.qcond.wait_for(lambda: len(self.q), timeout=1)
                log.debug(f'have {len(self.q)} audit records')
                audit_recs = list(self.q)
                self.q.clear()
                self.qlock.release()
                log.debug(f'recording {len(audit_recs)} audit records')
                with self.conn_lock:
                    try:
                        self.dbconn.execute("BEGIN")
                        self.dbconn.executemany(INSERT_AUDIT, audit_recs)
                        self.dbconn.commit()
                    except Exception as e:
                        log.error(f'exception: {e}')
                    finally:
                        self.dbconn.rollback()
                        self.qlock.acquire()
        except Exception as e:
            log.error(f'exception: {e}')

    def get_last_recorded_epoch(self):
        """
        Fetches the epoch from the very last row inserted into the database.
        """
        # Using 'rowid' is an optimization in SQLite if you don't have
        # an explicit AUTOINCREMENT primary key.
        query = "SELECT epoch FROM audit_commands ORDER BY rowid DESC LIMIT 1"

        try:
            with self.conn_lock:
                cursor = self.dbconn.cursor()
                cursor.execute(query)
                result = cursor.fetchone()

                if result is not None:
                    last_epoch = int(result[0])
                    log.info(f"Last recorded epoch found: {last_epoch}")
                    return last_epoch
                log.info("Audit table is empty. Starting fresh.")
                return 0
        except Exception as e:
            self.log.error(f"Error retrieving last recorded epoch: {e}")
            return 0

    def init_databases(self):
        CREATE_TABLES = '''CREATE TABLE IF NOT EXISTS audit_meta(
            key TEXT PRIMARY KEY,
            value NOT NULL
        ) WITHOUT ROWID;
        INSERT OR IGNORE INTO audit_meta (key, value) VALUES
            ('__db_version__', 1);
        CREATE TABLE IF NOT EXISTS audit_commands(
            id INTEGER PRIMARY KEY ASC,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            user TEXT NOT NULL,
            user_host TEXT NOT NULL,
            entity_name TEXT NOT NULL,
            command TEXT NOT NULL,
            args TEXT,
            state TEXT NOT NULL,
            retval INT NOT NULL,
            priority INT NOT NULL,
            sequence INT NOT NULL,
            epoch INT NOT NULL,
            from_name TEXT NOT NULL,
            from_rank INT NOT NULL,
            result_message TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON
            audit_commands(timestamp);
        CREATE INDEX IF NOT EXISTS idx_audit_epoch_seq ON
            audit_commands(epoch, sequence);
        CREATE INDEX IF NOT EXISTS idx_audit_entity ON
            audit_commands(entity_name);
        '''

        uri = f"file:///{self.AUDIT_POOL_NAME}:{self.MON_AUDIT_CMD_NS}/" \
            "audit_commands.db?vfs=ceph"
        log.debug(f'using uri: {uri}')

        self.conn_lock.acquire()
        try:
            db = sqlite3.connect(uri, check_same_thread=False, uri=True,
                                 isolation_level=None)
            db.execute('PRAGMA FOREIGN_KEYS = 1')
            db.execute('PRAGMA JOURNAL_MODE = PERSIST')
            db.execute('PRAGMA PAGE_SIZE = 65536')
            db.execute('PRAGMA CACHE_SIZE = 256')
            db.execute('PRAGMA TEMP_STORE = memory')
            db.executescript(CREATE_TABLES)
            self.dbconn = db
            log.debug('created tables')
        except Exception as e:
            log.error(f'exception: {e}')
        finally:
            self.conn_lock.release()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # audit queue
        self.dbconn = None
        self.qlock = Lock()
        self.conn_lock = Lock()
        self.qcond = Condition(self.qlock)
        self.stopping = False
        self.q = deque()

        # basic boilerplate...
        self.create_audit_pool()
        self.init_databases()

        # start record loop
        recorder = Thread(target=self.audit_recorder)
        recorder.start()

        # TODO: subscribe with last epoch. Note that ceph-mgr
        # already subscribes to log-info, but with starting
        # seq 0.
        self.get_last_recorded_epoch()
