/*
MIT License

Copyright (c) 2018 bert hubert

Permission is hereby granted, free of charge, to any person obtaining a copy
*/
#include "lmdb-safe.hh"

#include <fcntl.h>
#include <sys/stat.h>

#include <cstring>
#include <map>
#include <memory>
#include <mutex>

using namespace std;

namespace LMDBSafe {

MDBDbi::MDBDbi(MDB_env *env, MDB_txn *txn, const string_view dbname, unsigned int flags)
{
    (void)env;
    // A transaction that uses this function must finish (either commit or abort) before any other transaction in the process may use this function.

    if (const auto rc = mdb_dbi_open(txn, dbname.empty() ? 0 : &dbname[0], flags, &d_dbi))
        throw LMDBError("Unable to open named database: ", rc);

    // Database names are keys in the unnamed database, and may be read but not written.
}

MDBEnv::MDBEnv(const char *fname, unsigned int flags, mdb_mode_t mode, MDB_dbi maxDBs)
{
    mdb_env_create(&d_env);
    if (const auto rc = mdb_env_set_mapsize(d_env, 16ULL * 4096 * 244140ULL)) { // 4GB
        throw LMDBError("Setting map size: ", rc);
    }
    // Various other options may also need to be set before opening the handle, e.g. mdb_env_set_mapsize(), mdb_env_set_maxreaders(), mdb_env_set_maxdbs(),
    if (const auto rc = mdb_env_set_maxdbs(d_env, maxDBs)) {
        throw LMDBError("Setting maxdbs: ", rc);
    }

    // we need MDB_NOTLS since we rely on its semantics
    if (const auto rc = mdb_env_open(d_env, fname, flags | MDB_NOTLS, mode)) {
        // If this function fails, mdb_env_close() must be called to discard the MDB_env handle.
        mdb_env_close(d_env);
        throw LMDBError("Unable to open database file " + std::string(fname) + ": ", rc);
    }
}

void MDBEnv::incROTX()
{
    std::lock_guard<std::mutex> l(d_countmutex);
    ++d_ROtransactionsOut[std::this_thread::get_id()];
}

void MDBEnv::decROTX()
{
    std::lock_guard<std::mutex> l(d_countmutex);
    --d_ROtransactionsOut[std::this_thread::get_id()];
}

void MDBEnv::incRWTX()
{
    std::lock_guard<std::mutex> l(d_countmutex);
    ++d_RWtransactionsOut[std::this_thread::get_id()];
}

void MDBEnv::decRWTX()
{
    std::lock_guard<std::mutex> l(d_countmutex);
    --d_RWtransactionsOut[std::this_thread::get_id()];
}

int MDBEnv::getRWTX()
{
    std::lock_guard<std::mutex> l(d_countmutex);
    return d_RWtransactionsOut[std::this_thread::get_id()];
}
int MDBEnv::getROTX()
{
    std::lock_guard<std::mutex> l(d_countmutex);
    return d_ROtransactionsOut[std::this_thread::get_id()];
}

std::shared_ptr<MDBEnv> getMDBEnv(const char *fname, unsigned int flags, mdb_mode_t mode, MDB_dbi maxDBs)
{
    struct Value {
        weak_ptr<MDBEnv> wp;
        unsigned int flags;
    };

    static std::map<tuple<dev_t, ino_t>, Value> s_envs;
    static std::mutex mut;

    struct stat statbuf;
    if (stat(fname, &statbuf)) {
        if (errno != ENOENT)
            throw LMDBError("Unable to stat prospective mdb database: " + string(strerror(errno)));
        else {
            std::lock_guard<std::mutex> l(mut);
            auto fresh = std::make_shared<MDBEnv>(fname, flags, mode, maxDBs);
            if (stat(fname, &statbuf))
                throw LMDBError("Unable to stat prospective mdb database: " + string(strerror(errno)));
            auto key = std::tie(statbuf.st_dev, statbuf.st_ino);
            s_envs[key] = { fresh, flags };
            return fresh;
        }
    }

    std::lock_guard<std::mutex> l(mut);
    auto key = std::tie(statbuf.st_dev, statbuf.st_ino);
    auto iter = s_envs.find(key);
    if (iter != s_envs.end()) {
        auto sp = iter->second.wp.lock();
        if (sp) {
            if (iter->second.flags != flags)
                throw LMDBError("Can't open mdb with differing flags");

            return sp;
        } else {
            s_envs.erase(iter); // useful if make_shared fails
        }
    }

    auto fresh = std::make_shared<MDBEnv>(fname, flags, mode, maxDBs);
    s_envs[key] = { fresh, flags };

    return fresh;
}

MDBDbi MDBEnv::openDB(const string_view dbname, unsigned int flags)
{
    unsigned int envflags;
    mdb_env_get_flags(d_env, &envflags);
    /*
    This function must not be called from multiple concurrent transactions in the same process. A transaction that uses this function must finish (either commit or abort) before any other transaction in the process may use this function.
  */
    std::lock_guard<std::mutex> l(d_openmut);

    if (!(envflags & MDB_RDONLY)) {
        auto rwt = getRWTransaction();
        MDBDbi ret = rwt->openDB(dbname, flags);
        rwt->commit();
        return ret;
    }

    MDBDbi ret;
    {
        auto rwt = getROTransaction();
        ret = rwt->openDB(dbname, flags);
    }
    return ret;
}

MDBRWTransactionImpl::MDBRWTransactionImpl(MDBEnv *parent, MDB_txn *txn)
    : MDBROTransactionImpl(parent, txn)

{
}

MDB_txn *MDBRWTransactionImpl::openRWTransaction(MDBEnv *env, MDB_txn *parent, unsigned int flags)
{
    MDB_txn *result;
    if (env->getRWTX())
        throw LMDBError("Duplicate RW transaction");

    for (int tries = 0; tries < 3; ++tries) { // it might happen twice, who knows
        if (int rc = mdb_txn_begin(env->d_env, parent, flags, &result)) {
            if (rc == MDB_MAP_RESIZED && tries < 2) {
                // "If the mapsize is increased by another process (..) mdb_txn_begin() will return MDB_MAP_RESIZED.
                // call mdb_env_set_mapsize with a size of zero to adopt the new size."
                mdb_env_set_mapsize(env->d_env, 0);
                continue;
            }
            throw LMDBError("Unable to start RW transaction: ", rc);
        }
        break;
    }
    env->incRWTX();
    return result;
}

MDBRWTransactionImpl::MDBRWTransactionImpl(MDBEnv *parent, unsigned int flags)
    : MDBRWTransactionImpl(parent, openRWTransaction(parent, nullptr, flags))
{
}

MDBRWTransactionImpl::~MDBRWTransactionImpl()
{
    MDBRWTransactionImpl::abort();
}

void MDBRWTransactionImpl::commit()
{
    closeRORWCursors();
    if (!d_txn) {
        return;
    }

    if (const auto rc = mdb_txn_commit(d_txn)) {
        throw LMDBError("Committing transaction: ", rc);
    }
    environment().decRWTX();
    d_txn = nullptr;
}

void MDBRWTransactionImpl::abort()
{
    closeRORWCursors();
    if (!d_txn) {
        return;
    }

    mdb_txn_abort(d_txn);
    // prevent the RO destructor from cleaning up the transaction itself
    environment().decRWTX();
    d_txn = nullptr;
}

MDBROTransactionImpl::MDBROTransactionImpl(MDBEnv *parent, MDB_txn *txn)
    : d_parent(parent)
    , d_cursors()
    , d_txn(txn)
{
}

MDB_txn *MDBROTransactionImpl::openROTransaction(MDBEnv *env, MDB_txn *parent, unsigned int flags)
{
    if (env->getRWTX())
        throw LMDBError("Duplicate RO transaction");

    /*
    A transaction and its cursors must only be used by a single thread, and a thread may only have a single transaction at a time. If MDB_NOTLS is in use, this does not apply to read-only transactions. */
    MDB_txn *result = nullptr;
    for (int tries = 0; tries < 3; ++tries) { // it might happen twice, who knows
        if (const auto rc = mdb_txn_begin(env->d_env, parent, MDB_RDONLY | flags, &result)) {
            if (rc == MDB_MAP_RESIZED && tries < 2) {
                // "If the mapsize is increased by another process (..) mdb_txn_begin() will return MDB_MAP_RESIZED.
                // call mdb_env_set_mapsize with a size of zero to adopt the new size."
                mdb_env_set_mapsize(env->d_env, 0);
                continue;
            }
            throw LMDBError("Unable to start RO transaction: ", rc);
        }
        break;
    }
    env->incROTX();

    return result;
}

void MDBROTransactionImpl::closeROCursors()
{
    // we need to move the vector away to ensure that the cursors don’t mess with our iteration.
    std::vector<MDBROCursor *> buf;
    std::swap(d_cursors, buf);
    for (auto &cursor : buf) {
        cursor->close();
    }
}

MDBROTransactionImpl::MDBROTransactionImpl(MDBEnv *parent, unsigned int flags)
    : MDBROTransactionImpl(parent, openROTransaction(parent, nullptr, flags))
{
}

MDBROTransactionImpl::~MDBROTransactionImpl()
{
    // this is safe because C++ will not call overrides of virtual methods in destructors.
    MDBROTransactionImpl::commit();
}

void MDBROTransactionImpl::abort()
{
    closeROCursors();
    // if d_txn is non-nullptr here, either the transaction object was invalidated earlier (e.g. by moving from it), or it is an RW transaction which has already cleaned up the d_txn pointer (with an abort).
    if (d_txn) {
        d_parent->decROTX();
        mdb_txn_abort(d_txn); // this appears to work better than abort for r/o database opening
        d_txn = nullptr;
    }
}

void MDBROTransactionImpl::commit()
{
    closeROCursors();
    // if d_txn is non-nullptr here, either the transaction object was invalidated earlier (e.g. by moving from it), or it is an RW transaction which has already cleaned up the d_txn pointer (with an abort).
    if (d_txn) {
        d_parent->decROTX();
        if (const auto rc = mdb_txn_commit(d_txn)) { // this appears to work better than abort for r/o database opening
            throw LMDBError("Error committing transaction: ", rc);
        }
        d_txn = nullptr;
    }
}

void MDBRWTransactionImpl::clear(MDB_dbi dbi)
{
    if (const auto rc = mdb_drop(d_txn, dbi, 0)) {
        throw LMDBError("Error clearing database: ", rc);
    }
}

MDBRWCursor MDBRWTransactionImpl::getRWCursor(const MDBDbi &dbi)
{
    MDB_cursor *cursor;
    ;
    if (const auto rc = mdb_cursor_open(d_txn, dbi, &cursor)) {
        throw LMDBError("Error creating RO cursor: ", rc);
    }
    return MDBRWCursor(d_rw_cursors, cursor);
}

MDBRWCursor MDBRWTransactionImpl::getCursor(const MDBDbi &dbi)
{
    return getRWCursor(dbi);
}

MDBRWTransaction MDBRWTransactionImpl::getRWTransaction()
{
    MDB_txn *txn;
    if (const auto rc = mdb_txn_begin(environment(), *this, 0, &txn)) {
        throw LMDBError("Failed to start child transaction: ", rc);
    }
    // we need to increase the counter here because commit/abort on the child transaction will decrease it
    environment().incRWTX();
    return MDBRWTransaction(new MDBRWTransactionImpl(&environment(), txn));
}

MDBROTransaction MDBRWTransactionImpl::getROTransaction()
{
    return getRWTransaction();
}

MDBROTransaction MDBEnv::getROTransaction()
{
    return MDBROTransaction(new MDBROTransactionImpl(this));
}
MDBRWTransaction MDBEnv::getRWTransaction()
{
    return MDBRWTransaction(new MDBRWTransactionImpl(this));
}

void MDBRWTransactionImpl::closeRWCursors()
{
    decltype(d_rw_cursors) buf;
    std::swap(d_rw_cursors, buf);
    for (auto &cursor : buf) {
        cursor->close();
    }
}

MDBROCursor MDBROTransactionImpl::getCursor(const MDBDbi &dbi)
{
    return getROCursor(dbi);
}

MDBROCursor MDBROTransactionImpl::getROCursor(const MDBDbi &dbi)
{
    MDB_cursor *cursor;
    if (const auto rc = mdb_cursor_open(d_txn, dbi, &cursor)) {
        throw LMDBError("Error creating RO cursor: ", rc);
    }
    return MDBROCursor(d_cursors, cursor);
}

} // namespace LMDBSafe
