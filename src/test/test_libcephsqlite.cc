/* g++ -I ../src -I ../src/include/ -L ./lib/ -lcephsqlite -lsqlite3 -lrados -lradosstriper -o test_libsqlite ../src/test/test_libcephsqlite.cc
 */
#include <string.h>

#include "rados/librados.hpp"
#include "radosstriper/libradosstriper.hpp"
#include "libcephsqlite.h"

#include <iostream>
#include <fstream>
#include <string>

#include <sqlite3.h>

void create_db(sqlite3 *db);
void insert_db(sqlite3 *db, const char *file_name);
long int count_db(sqlite3 *db);
void list_db(sqlite3 *db);
void delete_db(sqlite3 *db);
const char *sqlite_error_str(int err);
const char *sqlite_exterror_str(int err);


int main(int argc, char **argv)
{
    int ret = -1;
    librados::Rados cluster;

    ret = cluster.init2("client.admin", "ceph", 0);
    if( ret < 0)
    {
        std::cerr << "Couldn't init cluster "<< ret << std::endl;
    }

    // make sure ceph.conf is in /etc/ceph/ and is world readable
    ret = cluster.conf_read_file("ceph.conf");
    if( ret < 0)
    {
        std::cerr << "Couldn't read conf file "<< ret << std::endl;
    }

    ret = cluster.connect();
    if(ret < 0)
    {
        std::cerr << "Couldn't connect to cluster "<< ret << std::endl;
    }
    else
    {
        std::cout << "Connected to Cluster"<< std::endl;
    }

    std::string cmd = (argv[1] ? argv[1] : "");
    int pool_id = 2; // for cephfs.a.data

    std::cout << "cmd:" << cmd << std::endl;
    sqlite3 *db = ceph_sqlite3_open(cluster, "rados-db-test", "DATABASE", pool_id, (cmd == "create"));

    ceph_sqlite3_set_db_params("rados-db-test", 3, (1<<22));

    if (!db) {
        std::cerr << "error: while initializing database" << std::endl;
        return 1;
    }

    if (cmd == "create") {
        create_db(db);
    } else if (cmd == "insert") {
        insert_db(db, argv[2]); /* argv[2] contains file name of data file */
    } else if (cmd == "count") {
        std::cout << "total rows: " << count_db(db) << std::endl;
    } else if (cmd == "list") {
        list_db(db);
    } else if (cmd == "delete") {
        delete_db(db);
    } else {
        std::cout << "Usage:" << std::endl;
        std::cout << "\t" << argv[0] << " {create|insert <file-name>|count|list|delete}" << std::endl;
    }

    sqlite3_close(db);
    cluster.shutdown();
    return 0;
}

inline
void dump_error(const char *func_name, int line, const char *errmsg, const char *sqlite_errmsg)
{
    std::cerr << func_name << ":" << std::dec << line << ":" << errmsg << ":" << sqlite_errmsg << std::endl;
}

void create_db(sqlite3 *db)
{
    const char *ddl1 =
        "CREATE TABLE IF NOT EXISTS t1(fname TEXT PRIMARY KEY NOT NULL, sha256sum TEXT NOT NULL)";
    const char *ddl2 =
        "CREATE UNIQUE INDEX t1fname ON t1(fname);";
    sqlite3_stmt *stmt = NULL;
    const char *unused = NULL;

    int ret = sqlite3_prepare_v2(db, ddl1, strlen(ddl1), &stmt, &unused);
    std::cerr << __FUNCTION__ << ": prepare:0x" << std::hex << ret << "(" << sqlite_error_str(ret) << ")" << std::endl;
    if (ret != SQLITE_OK) {
        dump_error(__FUNCTION__, __LINE__, "error: when preparing", sqlite3_errmsg(db));
        goto out;
    }

    dump_error(__FUNCTION__, __LINE__, "stepping", "");
    ret = sqlite3_step(stmt);
    std::cerr << __FUNCTION__ << ": step:0x" << std::hex << ret << "(" << sqlite_error_str(ret) << ")" << std::endl;
    if (ret != SQLITE_DONE) {
        dump_error(__FUNCTION__, __LINE__, "error: when stepping", sqlite3_errmsg(db));
        goto out;
    }

#if 0
    sqlite3_finalize(stmt);

    ret = sqlite3_prepare_v2(db, ddl2, strlen(ddl2), &stmt, &unused);
    std::cerr << __FUNCTION__ << ": prepare:0x" << std::hex << ret << "(" << sqlite_error_str(ret) << ")" << std::endl;
    if (ret != SQLITE_OK) {
        dump_error(__FUNCTION__, __LINE__, "error: when preparing", sqlite3_errmsg(db));
        goto out;
    }

    ret = sqlite3_step(stmt);
    std::cerr << __FUNCTION__ << ": step:0x" << std::hex << ret << "(" << sqlite_error_str(ret) << ")" << std::endl;
    if (ret != SQLITE_DONE) {
        dump_error(__FUNCTION__, __LINE__, "error: when stepping", sqlite3_errmsg(db));
    }
#endif
out:
    sqlite3_finalize(stmt);
}

void insert_db(sqlite3 *db, const char *file_name)
{
    int row = 0;
    std::string col_fname;
    std::string col_sha256sum;

    std::ifstream is(file_name);

    const char *dml = "INSERT INTO t1(fname, sha256sum) VALUES(?,?);";
    sqlite3_stmt *stmt = NULL;
    const char *unused = NULL;

    while (is >> col_sha256sum >> col_fname) {
        if (sqlite3_prepare_v2(db, dml, strlen(dml), &stmt, &unused) != SQLITE_OK) {
            dump_error(__FUNCTION__, __LINE__, "error: while preparing", sqlite3_errmsg(db));
            goto out;
        }

        if (sqlite3_bind_text(stmt, 1, col_fname.c_str(), strlen(col_fname.c_str()), NULL) != SQLITE_OK) {
            std::stringstream ss;

            ss << "error: while attempting to sqlite3_bind_text(col_fname) for row " << row;
            dump_error(__FUNCTION__, __LINE__, ss.str().c_str(), sqlite3_errmsg(db));
            goto out;
        }
        if (sqlite3_bind_text(stmt, 2, col_sha256sum.c_str(), strlen(col_sha256sum.c_str()), NULL) != SQLITE_OK) {
            std::stringstream ss;

            ss << "error: while attempting to sqlite3_bind_text(col_sha256sum) for row " << row;
            dump_error(__FUNCTION__, __LINE__, ss.str().c_str(), sqlite3_errmsg(db));
            goto out;
        }

        int retries = 20;
        int ret = -1;
        while ((ret = sqlite3_step(stmt)) != SQLITE_DONE) {
            usleep(1000);
            retries--;
        }
        if (ret != SQLITE_DONE) {
            std::stringstream ss;

            ss << "error:0x" << std::hex << ret << " while attempting to sqlite3_step() for row " << row;
            dump_error(__FUNCTION__, __LINE__, ss.str().c_str(), sqlite3_errmsg(db));
            goto out;
        }
        if (sqlite3_reset(stmt) != SQLITE_OK) {
            std::stringstream ss;

            ss << "error: while resetting for row " << row;
            dump_error(__FUNCTION__, __LINE__, ss.str().c_str(), sqlite3_errmsg(db));
            goto out;
        }
        sqlite3_finalize(stmt);
        ++row;
        std::cerr << "inserted row: " << row << std::endl;
    }
out:
    return;
}

long int count_db(sqlite3 *db)
{
    long        ret = -1;
    const char *dml = "SELECT COUNT(*) FROM t1;";
    sqlite3_stmt *stmt = NULL;
    const char *unused = NULL;

    if (sqlite3_prepare_v2(db, dml, strlen(dml), &stmt, &unused) != SQLITE_OK) {
        dump_error(__FUNCTION__, __LINE__, "error: when preparing", sqlite3_errmsg(db));
        goto out;
    }

    if (sqlite3_step(stmt) != SQLITE_ROW) {
        std::stringstream ss;

        ss << "error: while stepping";
        dump_error(__FUNCTION__, __LINE__, ss.str().c_str(), sqlite3_errmsg(db));
        goto out;
    }
    ret = sqlite3_column_int64(stmt, 0);
out:
    sqlite3_finalize(stmt);
    return ret;
}

void list_db(sqlite3 *db)
{
    const char *dml = "SELECT * FROM t1;";
    sqlite3_stmt *stmt = NULL;
    const char *unused = NULL;

    if (sqlite3_prepare_v2(db, dml, strlen(dml), &stmt, &unused) != SQLITE_OK) {
        dump_error(__FUNCTION__, __LINE__, "error: when preparing", sqlite3_errmsg(db));
        goto out;
    }

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        std::cout << sqlite3_column_text(stmt, 1) << " " << sqlite3_column_text(stmt, 0) << std::endl;
    }
out:
    sqlite3_finalize(stmt);
}

void delete_db(sqlite3 *db)
{
    const char *dml = "DELETE FROM t1; DROP INDEX t1fname; DROP TABLE t1;";
    sqlite3_stmt *stmt = NULL;
    const char *unused = NULL;

    if (sqlite3_prepare_v2(db, dml, strlen(dml), &stmt, &unused) != SQLITE_OK) {
        dump_error(__FUNCTION__, __LINE__, "error: when preparing", sqlite3_errmsg(db));
        goto out;
    }

    if (sqlite3_step(stmt) != SQLITE_DONE) {
        dump_error(__FUNCTION__, __LINE__, "error: while deleting table", sqlite3_errmsg(db));
    }
out:
    sqlite3_finalize(stmt);
}

#define CASE(x) case x: return #x

const char *sqlite_error_str(int err)
{
    switch (err & 0xff) {
        CASE(SQLITE_OK);
        CASE(SQLITE_ERROR);
        CASE(SQLITE_INTERNAL);
        CASE(SQLITE_PERM);
        CASE(SQLITE_ABORT);
        CASE(SQLITE_BUSY);
        CASE(SQLITE_LOCKED);
        CASE(SQLITE_NOMEM);
        CASE(SQLITE_READONLY);
        CASE(SQLITE_INTERRUPT);
        CASE(SQLITE_IOERR);
        CASE(SQLITE_CORRUPT);
        CASE(SQLITE_NOTFOUND);
        CASE(SQLITE_FULL);
        CASE(SQLITE_CANTOPEN);
        CASE(SQLITE_PROTOCOL);
        CASE(SQLITE_EMPTY);
        CASE(SQLITE_SCHEMA);
        CASE(SQLITE_TOOBIG);
        CASE(SQLITE_CONSTRAINT);
        CASE(SQLITE_MISMATCH);
        CASE(SQLITE_MISUSE);
        CASE(SQLITE_NOLFS);
        CASE(SQLITE_AUTH);
        CASE(SQLITE_FORMAT);
        CASE(SQLITE_RANGE);
        CASE(SQLITE_NOTADB);
        CASE(SQLITE_NOTICE);
        CASE(SQLITE_WARNING);
        CASE(SQLITE_ROW);
        CASE(SQLITE_DONE);
    }
    return "NULL";
}

const char *sqlite_exterror_str(int err)
{
    switch (err & 0xff) {
        case SQLITE_ERROR:
            switch (err) {
#ifdef SQLITE_ERROR_MISSING_COLLSEQ
                CASE(SQLITE_ERROR_MISSING_COLLSEQ);
#endif
#ifdef SQLITE_ERROR_RETRY
                CASE(SQLITE_ERROR_RETRY);
#endif
#ifdef SQLITE_ERROR_SNAPSHOT
                CASE(SQLITE_ERROR_SNAPSHOT);
#endif
            }
            break;

        case SQLITE_IOERR:
            switch (err) {
                CASE(SQLITE_IOERR_READ);
                CASE(SQLITE_IOERR_SHORT_READ);
                CASE(SQLITE_IOERR_WRITE);
                CASE(SQLITE_IOERR_FSYNC);
                CASE(SQLITE_IOERR_DIR_FSYNC);
                CASE(SQLITE_IOERR_TRUNCATE);
                CASE(SQLITE_IOERR_FSTAT);
                CASE(SQLITE_IOERR_UNLOCK);
                CASE(SQLITE_IOERR_RDLOCK);
                CASE(SQLITE_IOERR_DELETE);
                CASE(SQLITE_IOERR_BLOCKED);
                CASE(SQLITE_IOERR_NOMEM);
                CASE(SQLITE_IOERR_ACCESS);
                CASE(SQLITE_IOERR_CHECKRESERVEDLOCK);
                CASE(SQLITE_IOERR_LOCK);
                CASE(SQLITE_IOERR_CLOSE);
                CASE(SQLITE_IOERR_DIR_CLOSE);
                CASE(SQLITE_IOERR_SHMOPEN);
                CASE(SQLITE_IOERR_SHMSIZE);
                CASE(SQLITE_IOERR_SHMLOCK);
                CASE(SQLITE_IOERR_SHMMAP);
                CASE(SQLITE_IOERR_SEEK);
                CASE(SQLITE_IOERR_DELETE_NOENT);
                CASE(SQLITE_IOERR_MMAP);
                CASE(SQLITE_IOERR_GETTEMPPATH);
                CASE(SQLITE_IOERR_CONVPATH);
                CASE(SQLITE_IOERR_VNODE);
                CASE(SQLITE_IOERR_AUTH);
#ifdef SQLITE_IOERR_BEGIN_ATOMIC
                CASE(SQLITE_IOERR_BEGIN_ATOMIC);
#endif
#ifdef SQLITE_IOERR_COMMIT_ATOMIC
                CASE(SQLITE_IOERR_COMMIT_ATOMIC);
#endif
#ifdef SQLITE_IOERR_ROLLBACK_ATOMIC
                CASE(SQLITE_IOERR_ROLLBACK_ATOMIC);
#endif
            }
            break;

        case SQLITE_LOCKED:
            switch (err) {
                CASE(SQLITE_LOCKED_SHAREDCACHE);
#ifdef SQLITE_LOCKED_VTAB
                CASE(SQLITE_LOCKED_VTAB);
#endif
            }
            break;

        case SQLITE_BUSY:
            switch (err) {
                CASE(SQLITE_BUSY_RECOVERY);
                CASE(SQLITE_BUSY_SNAPSHOT);
            }
            break;

        case SQLITE_CANTOPEN:
            switch (err) {
                CASE(SQLITE_CANTOPEN_NOTEMPDIR);
                CASE(SQLITE_CANTOPEN_ISDIR);
                CASE(SQLITE_CANTOPEN_FULLPATH);
                CASE(SQLITE_CANTOPEN_CONVPATH);
#ifdef SQLITE_CANTOPEN_DIRTYWAL
                CASE(SQLITE_CANTOPEN_DIRTYWAL);
#endif
            }
            break;

        case SQLITE_CORRUPT:
            switch (err) {
                CASE(SQLITE_CORRUPT_VTAB);
#ifdef SQLITE_CORRUPT_SEQUENCE
                CASE(SQLITE_CORRUPT_SEQUENCE);
#endif
            }
            break;

        case SQLITE_READONLY:
            switch (err) {
                CASE(SQLITE_READONLY_RECOVERY);
                CASE(SQLITE_READONLY_CANTLOCK);
                CASE(SQLITE_READONLY_ROLLBACK);
                CASE(SQLITE_READONLY_DBMOVED);
#ifdef SQLITE_READONLY_CANTINIT
                CASE(SQLITE_READONLY_CANTINIT);
#endif
#ifdef SQLITE_READONLY_DIRECTORY
                CASE(SQLITE_READONLY_DIRECTORY);
#endif
            }
            break;

        case SQLITE_ABORT:
            switch (err) {
                CASE(SQLITE_ABORT_ROLLBACK);
            }
            break;

        case SQLITE_CONSTRAINT:
            switch (err) {
                CASE(SQLITE_CONSTRAINT_CHECK);
                CASE(SQLITE_CONSTRAINT_COMMITHOOK);
                CASE(SQLITE_CONSTRAINT_FOREIGNKEY);
                CASE(SQLITE_CONSTRAINT_FUNCTION);
                CASE(SQLITE_CONSTRAINT_NOTNULL);
                CASE(SQLITE_CONSTRAINT_PRIMARYKEY);
                CASE(SQLITE_CONSTRAINT_TRIGGER);
                CASE(SQLITE_CONSTRAINT_UNIQUE);
                CASE(SQLITE_CONSTRAINT_VTAB);
                CASE(SQLITE_CONSTRAINT_ROWID);
            }
            break;

        case SQLITE_NOTICE:
            switch (err) {
                CASE(SQLITE_NOTICE_RECOVER_WAL);
                CASE(SQLITE_NOTICE_RECOVER_ROLLBACK);
            }
            break;

        case SQLITE_WARNING:
            switch (err) {
                CASE(SQLITE_WARNING_AUTOINDEX);
            }
            break;

        case SQLITE_AUTH:
            switch (err) {
                CASE(SQLITE_AUTH_USER);
            }
            break;

        case SQLITE_OK:
            switch (err) {
#ifdef SQLITE_OK_LOAD_PERMANENTLY
                CASE(SQLITE_OK_LOAD_PERMANENTLY);
#endif
            }
            break;
    }
    return "EXTNULL";
}
