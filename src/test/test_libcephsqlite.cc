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

#define sqlcatchcode(S, code) \
do {\
    rc = S;\
    if (rc != code) {\
        if (rc == SQLITE_BUSY) {\
            rc = EAGAIN;\
        } else {\
            std::cerr << "[" << __FILE__ << ":" << std::dec << __LINE__ << "]"\
                      << " sqlite3 error: " << rc << " `" << sqlite3_errstr(rc)\
                      << "': " << sqlite3_errmsg(db) << std::endl;\
            if (rc == SQLITE_CONSTRAINT) {\
                rc = EINVAL;\
            } else {\
                rc = EIO;\
            }\
        }\
        sqlite3_finalize(stmt);\
        stmt = NULL;\
        goto out;\
    }\
} while (0)

#define sqlcatch(S) sqlcatchcode(S, 0)
void usage();
void create_db(sqlite3 *db);
void insert_rand(sqlite3 *db, uint64_t count, uint64_t size);
void insert_db(sqlite3 *db, const char *file_name);
long int count_db(sqlite3 *db, char *table_name);
void list_db(sqlite3 *db, char *table_name);
void delete_db(sqlite3 *db, char *table_name);
const char *sqlite_error_str(int err);
const char *sqlite_exterror_str(int err);
std::vector<char *> get_args(char *line);


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
        return 1;
    }
    else
    {
        std::cout << "Connected to Cluster"<< std::endl;
    }

    std::string test = "unknown"; // "local" or "rados"
    char line[128];
    sqlite3 *db = NULL;
    sqlite3_stmt *stmt = NULL;
    char *sErrMsg = NULL;
    int rc = -1;

    while (std::cout << "\ncmd > ", std::cin.getline(line, sizeof(line) - 1), strcmp(line, "quit") != 0) {
        std::vector<char*> args = get_args(line);

        std::string cmd = (args[1] ? args[1] : "");
        int pool_id = 2; // for cephfs.a.data in the vstart cluster

        std::cout << "cmd:" << cmd << std::endl;
        std::cout << "test:" << test << std::endl;
        if (!db) {
            if (test == "local") {
                bool must_create = (cmd == "create");
                int db_open_flags = SQLITE_OPEN_NOMUTEX       | /* single client access */
                                    SQLITE_OPEN_PRIVATECACHE  |
                                    SQLITE_OPEN_READWRITE     |
                                    (must_create ? SQLITE_OPEN_CREATE : 0);

                sqlcatch(sqlite3_open_v2("rados-db-test.db", &db, db_open_flags, "unix"));
            } else if (test == "rados") {
                db = ceph_sqlite3_open(cluster, "rados-db-test", "DATABASE", pool_id, (cmd == "create"));
            } else {
                if (cmd != "test"){
                    std::cerr << "please specify the 'test' command before any other command" << std::endl;
                    usage();
                    continue;
                }
            }
        }

        if ((cmd != "test") && !db) {
            std::cerr << "error: while initializing database" << std::endl;
            return 1;
        }

        struct timespec start, end;

        clock_gettime(CLOCK_MONOTONIC, &start);

        if (cmd == "create") {
            create_db(db);
            if (db) {
              sqlcatch(sqlite3_exec(db, "PRAGMA page_size = 4096", NULL, NULL, &sErrMsg));
              //ceph_sqlite3_set_db_page_size(db, 65536);
              sqlcatch(sqlite3_exec(db, "PRAGMA synchronous = ON", NULL, NULL, &sErrMsg));
              //sqlcatch(sqlite3_exec(db, "PRAGMA journal_mode = WAL", NULL, NULL, &sErrMsg));
              sqlcatch(sqlite3_exec(db, "PRAGMA cache_size = 2048", NULL, NULL, &sErrMsg));

              std::cerr << "db name:" << sqlite3_db_filename(db, "main") << std::endl;
            }
            ceph_sqlite3_set_db_params("rados-db-test", 3, (1<<22));
        } else if (cmd == "insert_rand") {
            if (args[2] == NULL || args[3] == NULL )
                std::cerr << "*** please specify the number of records to insert followed by the record size" << std::endl;
            else
                insert_rand(db, strtoul(args[2], NULL, 10), strtoul(args[3], NULL, 10));
        } else if (cmd == "insert") {
            if (args[2] == NULL)
                std::cerr << "*** please specify the name of the file to read from" << std::endl;
            else
                insert_db(db, args[2]); /* args[2] contains file name of data file */
        } else if (cmd == "count") {
            if (args[2] == NULL)
                std::cerr << "*** please specify the name of the table to read from" << std::endl;
            else {
                // args[2] is the table name
                long rows = count_db(db, args[2]);
                std::cout << "total rows: " << rows << std::endl;
            }
        } else if (cmd == "list") {
            if (args[2] == NULL)
                std::cerr << "*** please specify the name of the table to read from" << std::endl;
            else {
                // args[2] is the table name
                list_db(db, args[2]);
            }
        } else if (cmd == "delete") {
            if (args[2] == NULL)
                std::cerr << "*** please specify the name of the table to delete from" << std::endl;
            else {
                // args[2] is the table name
                delete_db(db, args[2]);
            }
        } else if (cmd == "dump") {
            ceph_sqlite3_dump_buffer_cache(db, "rados-db-test.db");
        } else if (cmd == "test") {
            /* set the test type:
             * "local" implies the database should be created locally
             * "rados" implies the database should be created inside the default
             *         data pool in the vstart cluster
             */
            test = args[2];
        } else if (cmd == "help" || cmd == "") {
            usage();
        } else {
            usage();
        }
        clock_gettime(CLOCK_MONOTONIC, &end);
        struct timespec diff;
        diff.tv_sec = end.tv_sec - start.tv_sec;
        diff.tv_nsec = end.tv_nsec - start.tv_nsec;
        if (diff.tv_nsec < 0) {
            diff.tv_sec--;
            diff.tv_nsec = 1000000000 + diff.tv_nsec;
        }
        std::cout << "elapsed: " << diff.tv_sec << "." << std::setw(9) << std::setfill('0') << diff.tv_nsec << std::endl;
        line[0] = 0;
    }

    sqlite3_close(db);
    cluster.shutdown();
out:
    return 0;
}

void usage()
{
    std::cout << "Usage:" << std::endl;
    std::cout << "\t" << "{create|insert_rand <count> <size>|insert <file-name>|count {t1|rand}|list {t1|rand}|delete {t1|rand}}" << std::endl;
    std::cout << "\t" << "test {local|rados}" << std::endl;
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
    /*
    const char *ddl2 =
        "CREATE UNIQUE INDEX t1fname ON t1(fname);";
    */
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

char blob[(5<<20) + 1];
void insert_rand(sqlite3 *db, uint64_t count, uint64_t size)
{
#if 0
    static const char SQL[] = {
      "CREATE TABLE IF NOT EXISTS rand(text BLOB NOT NULL);"
      "INSERT INTO rand VALUES (randomblob(?));"
    };

    const char *current = SQL;
#else
    std::string sSQL = {
      "CREATE TABLE IF NOT EXISTS rand(text BLOB NOT NULL);"
      "INSERT INTO rand VALUES "
    };

    memset(blob, 'A', 5<<20);
    blob[5<<20] = 0;
    sSQL += "('";
    sSQL += blob;
    sSQL += "');";

    const char *current = sSQL.c_str();
#endif
    char *sErrMsg = NULL;
    sqlite3_stmt *stmt = NULL;
    int rc;


    sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
    sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
    sqlcatch(sqlite3_finalize(stmt));

    sqlcatch(sqlite3_prepare_v2(db, current, -1, &stmt, &current));
    //sqlcatch(sqlite3_bind_int64(stmt, 1, (sqlite3_int64)size));
    sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, &sErrMsg);
    for (uint64_t i = 0; i < count; i++) {
      sqlcatchcode(sqlite3_step(stmt), SQLITE_DONE);
      std::cout << "last row inserted: " << std::dec << sqlite3_last_insert_rowid(db) << std::endl;
    }
    sqlite3_exec(db, "END TRANSACTION", NULL, NULL, &sErrMsg);
    sqlcatch(sqlite3_finalize(stmt));

out:
    (void)0;
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
    char *sErrMsg = NULL;

    if (sqlite3_prepare_v2(db, dml, strlen(dml), &stmt, &unused) != SQLITE_OK) {
        dump_error(__FUNCTION__, __LINE__, "error: while preparing", sqlite3_errmsg(db));
        goto out;
    }
    sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, &sErrMsg);
    while (is >> col_sha256sum >> col_fname) {

        if (sqlite3_bind_text(stmt, 1, col_fname.c_str(), col_fname.length(), SQLITE_STATIC) != SQLITE_OK) {
            std::stringstream ss;

            ss << "error: while attempting to sqlite3_bind_text(col_fname) for row " << row;
            dump_error(__FUNCTION__, __LINE__, ss.str().c_str(), sqlite3_errmsg(db));
            goto out;
        }
        if (sqlite3_bind_text(stmt, 2, col_sha256sum.c_str(), col_sha256sum.length(), SQLITE_STATIC) != SQLITE_OK) {
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
        sqlite3_clear_bindings(stmt);
        if (sqlite3_reset(stmt) != SQLITE_OK) {
            std::stringstream ss;

            ss << "error: while resetting for row " << row;
            dump_error(__FUNCTION__, __LINE__, ss.str().c_str(), sqlite3_errmsg(db));
            goto out;
        }
        ++row;
        std::cerr << "inserted row: " << std::dec << row << std::endl;
    }
    sqlite3_exec(db, "END TRANSACTION", NULL, NULL, &sErrMsg);
    sqlite3_finalize(stmt);
out:
    return;
}

long int count_db(sqlite3 *db, char *table_name)
{
    long        ret = -1;
    std::stringstream ss;
    ss << "SELECT COUNT(*) FROM " << table_name << ";";
    std::string sdml = ss.str();
    const char *dml = sdml.c_str();
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

void list_db(sqlite3 *db, char *table_name)
{
    std::stringstream ss;
    ss << "SELECT * FROM " << table_name << ";";
    std::string sdml = ss.str();
    const char *dml = sdml.c_str();
    sqlite3_stmt *stmt = NULL;
    const char *unused = NULL;

    if (sqlite3_prepare_v2(db, dml, strlen(dml), &stmt, &unused) != SQLITE_OK) {
        dump_error(__FUNCTION__, __LINE__, "error: when preparing", sqlite3_errmsg(db));
        goto out;
    }

    if (strcmp(table_name, "t1") == 0) {
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            std::cout << sqlite3_column_text(stmt, 1) << " " << sqlite3_column_text(stmt, 0) << std::endl;
        }
    } else {
        // for table "rand"
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            // std::cout << sqlite3_column_text(stmt, 0) << std::endl;
            std::cout << ".";
            std::cout.flush();
        }
        std::cout << std::endl;
    }
out:
    sqlite3_finalize(stmt);
}

void delete_db(sqlite3 *db, char *table_name)
{
    std::stringstream ss;
    ss << "DELETE FROM " << table_name << ";";
    //const char *dml = "DELETE FROM t1; DROP INDEX t1fname; DROP TABLE t1;";
    std::string sdml = ss.str();
    const char *dml = sdml.c_str();
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

static char dummy[] = {'D', 'U', 'M', 'M', 'Y', 0};

std::vector<char *> get_args(char *line)
{
    std::vector<char *> args;
    std::string::size_type n;
    std::string::size_type b = 0;

    std::string sline = line;

    args.push_back(dummy); // first argument is dummy
    while ((n = sline.find(" ", b)) != std::string::npos) {
        args.push_back(&line[b]);
        line[n] = 0;
        b = n + 1;
    }
    args.push_back(&line[b]);
    return args;
}
