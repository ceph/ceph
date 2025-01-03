/*
MIT License

Copyright (c) 2018 bert hubert

Permission is hereby granted, free of charge, to any person obtaining a copy
*/
#pragma once

#include "lmdb-safe-global.h"

#include <lmdb.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

/*!
 * \brief The LMDBSafe namespace contains all classes/types contained by the lmdb-safe and
 *        lmdb-typed libraries.
 * \remarks
 * - Error strategy: Anything that "should never happen" turns into an exception. But things
 *   like "duplicate entry" or "no such key" are for you to deal with.
 * - Thread safety: We are as safe as LMDB. You can talk to MDBEnv from as many threads as you
 *   want.
 */
namespace LMDBSafe {

// apple compiler somehow has string_view even in c++11!
#ifdef __cpp_lib_string_view
using std::string_view;
#else
#include <boost/version.hpp>
#if BOOST_VERSION > 105400
#include <boost/utility/string_view.hpp>
using boost::string_view;
#else
#include <boost/utility/string_ref.hpp>
using string_view = boost::string_ref;
#endif
#endif

/*!
 * \brief The LMDBError class is thrown when an error happens.
 */
class LMDB_SAFE_EXPORT LMDBError : public std::runtime_error {
public:
    explicit LMDBError(const std::string &error) noexcept
        : std::runtime_error(error)
        , ec(0)
    {
    }

    explicit LMDBError(const std::string &context, int error) noexcept
        : std::runtime_error(context + mdb_strerror(error))
        , ec(error)
    {
    }

    const int ec;
};

/*!
 * \brief The MDBDbi class is our only 'value type' object as 1) a dbi is actually an integer
 *        and 2) per LMDB documentation, we never close it.
 */
class LMDB_SAFE_EXPORT MDBDbi {
public:
    MDBDbi()
    {
        d_dbi = std::numeric_limits<decltype(d_dbi)>::max();
    }
    explicit MDBDbi(MDB_env *env, MDB_txn *txn, string_view dbname, unsigned int flags);

    operator const MDB_dbi &() const
    {
        return d_dbi;
    }

    MDB_dbi d_dbi;
};

class MDBRWTransactionImpl;
class MDBROTransactionImpl;
using MDBROTransaction = std::unique_ptr<MDBROTransactionImpl>;
using MDBRWTransaction = std::unique_ptr<MDBRWTransactionImpl>;

/*!
 * \brief The MDBEnv class is a handle to an MDB environment.
 */
class LMDB_SAFE_EXPORT MDBEnv {
public:
    MDBEnv(const char *fname, unsigned int flags, mdb_mode_t mode, MDB_dbi maxDBs = 10);

    /*!
     * \brief Closes the MDB environment.
     * \remarks Only a single thread may call this function. All transactions, databases, and cursors must already be closed
     *          before calling this function.
     */
    ~MDBEnv()
    {
        mdb_env_close(d_env);
    }

    MDBDbi openDB(const string_view dbname, unsigned int flags);

    MDBRWTransaction getRWTransaction();
    MDBROTransaction getROTransaction();

    operator MDB_env *&()
    {
        return d_env;
    }
    MDB_env *d_env;

    int getRWTX();
    void incRWTX();
    void decRWTX();
    int getROTX();
    void incROTX();
    void decROTX();

private:
    std::mutex d_openmut;
    std::mutex d_countmutex;
    std::map<std::thread::id, int> d_RWtransactionsOut;
    std::map<std::thread::id, int> d_ROtransactionsOut;
};

/*!
 * \brief Opens an MDB environment for the specified database file.
 */
LMDB_SAFE_EXPORT std::shared_ptr<MDBEnv> getMDBEnv(const char *fname, unsigned int flags, mdb_mode_t mode, MDB_dbi maxDBs = 128);

/*!
 * \brief The MDBOutVal struct is the handle to an MDB value used as output.
 */
struct LMDB_SAFE_EXPORT MDBOutVal {
    operator MDB_val &()
    {
        return d_mdbval;
    }

    template <class T, typename std::enable_if<std::is_arithmetic<T>::value, T>::type * = nullptr> const T get()
    {
        T ret;
        if (d_mdbval.mv_size != sizeof(T))
            throw LMDBError("MDB data has wrong length for type");

        memcpy(&ret, d_mdbval.mv_data, sizeof(T));
        return ret;
    }

    template <class T, typename std::enable_if<std::is_class<T>::value, T>::type * = nullptr> T get() const;

    template <class T> T get_struct() const
    {
        T ret;
        if (d_mdbval.mv_size != sizeof(T))
            throw LMDBError("MDB data has wrong length for type");

        memcpy(&ret, d_mdbval.mv_data, sizeof(T));
        return ret;
    }

    template <class T> const T *get_struct_ptr() const
    {
        if (d_mdbval.mv_size != sizeof(T))
            throw LMDBError("MDB data has wrong length for type");

        return reinterpret_cast<const T *>(d_mdbval.mv_data);
    }

    MDB_val d_mdbval;
};

template <> inline std::string MDBOutVal::get<std::string>() const
{
    return std::string(static_cast<char *>(d_mdbval.mv_data), d_mdbval.mv_size);
}

template <> inline string_view MDBOutVal::get<string_view>() const
{
    return string_view(static_cast<char *>(d_mdbval.mv_data), d_mdbval.mv_size);
}

/*!
 * \brief The MDBInVal struct is the handle to an MDB value used as input.
 */
class LMDB_SAFE_EXPORT MDBInVal {
public:
    MDBInVal(const MDBOutVal &rhs)
    {
        d_mdbval = rhs.d_mdbval;
    }

    template <class T, typename std::enable_if<std::is_arithmetic<T>::value, T>::type * = nullptr> MDBInVal(T i)
    {
        memcpy(&d_memory[0], &i, sizeof(i));
        d_mdbval.mv_size = sizeof(T);
        d_mdbval.mv_data = d_memory;
        ;
    }

    MDBInVal(const char *s)
    {
        d_mdbval.mv_size = strlen(s);
        d_mdbval.mv_data = static_cast<void *>(const_cast<char *>(s));
    }

    MDBInVal(string_view v)
    {
        d_mdbval.mv_size = v.size();
        d_mdbval.mv_data = static_cast<void *>(const_cast<char *>(v.data()));
    }

    MDBInVal(const std::string &v)
    {
        d_mdbval.mv_size = v.size();
        d_mdbval.mv_data = static_cast<void *>(const_cast<char *>(v.data()));
    }

    template <typename T> static MDBInVal fromStruct(const T &t)
    {
        MDBInVal ret;
        ret.d_mdbval.mv_size = sizeof(T);
        ret.d_mdbval.mv_data = static_cast<void *>(&const_cast<T &>(t));
        return ret;
    }

    operator MDB_val &()
    {
        return d_mdbval;
    }
    MDB_val d_mdbval;

private:
    MDBInVal()
    {
    }
    char d_memory[sizeof(double)];
};

class MDBROCursor;

/*!
 * \brief The MDBROTransactionImpl class wraps read operations.
 */
class LMDB_SAFE_EXPORT MDBROTransactionImpl {
protected:
    MDBROTransactionImpl(MDBEnv *parent, MDB_txn *txn);

private:
    static MDB_txn *openROTransaction(MDBEnv *env, MDB_txn *parent, unsigned int flags = 0);

    MDBEnv *d_parent;
    std::vector<MDBROCursor *> d_cursors;

protected:
    MDB_txn *d_txn;

    void closeROCursors();

public:
    explicit MDBROTransactionImpl(MDBEnv *parent, unsigned int flags = 0);

    MDBROTransactionImpl(const MDBROTransactionImpl &src) = delete;
    MDBROTransactionImpl &operator=(const MDBROTransactionImpl &src) = delete;

    // The move constructor/operator cannot be made safe due to Object Slicing with MDBRWTransaction.
    MDBROTransactionImpl(MDBROTransactionImpl &&rhs) = delete;
    MDBROTransactionImpl &operator=(MDBROTransactionImpl &&rhs) = delete;

    virtual ~MDBROTransactionImpl();

    virtual void abort();
    virtual void commit();

    int get(MDB_dbi dbi, const MDBInVal &key, MDBOutVal &val)
    {
        if (!d_txn)
            throw LMDBError("Attempt to use a closed RO transaction for get");

        const auto rc = mdb_get(d_txn, dbi, const_cast<MDB_val *>(&key.d_mdbval), &val.d_mdbval);
        if (rc && rc != MDB_NOTFOUND)
            throw LMDBError("Getting data: ", rc);

        return rc;
    }

    int get(MDB_dbi dbi, const MDBInVal &key, string_view &val)
    {
        MDBOutVal out;
        int rc = get(dbi, key, out);
        if (!rc)
            val = out.get<string_view>();
        return rc;
    }

    // this is something you can do, readonly
    MDBDbi openDB(string_view dbname, unsigned int flags)
    {
        return MDBDbi(d_parent->d_env, d_txn, dbname, flags);
    }

    MDBROCursor getCursor(const MDBDbi &);
    MDBROCursor getROCursor(const MDBDbi &);

    operator MDB_txn *()
    {
        return d_txn;
    }

    inline operator bool() const
    {
        return d_txn;
    }

    inline MDBEnv &environment()
    {
        return *d_parent;
    }
};

/*!
 * \brief The MDBGenCursor class represents a MDB_cursor handle.
 * \remarks
 * - A cursor in a read-only transaction must be closed explicitly, before or after its transaction ends.
 *   It can be reused with mdb_cursor_renew() before finally closing it.
 * - "If the parent transaction commits, the cursor must not be used again."
 */
template <class Transaction, class T> class MDBGenCursor {
private:
    std::vector<T *> *d_registry;
    MDB_cursor *d_cursor;

public:
    MDBGenCursor()
        : d_registry(nullptr)
        , d_cursor(nullptr)
    {
    }

    MDBGenCursor(std::vector<T *> &registry, MDB_cursor *cursor)
        : d_registry(&registry)
        , d_cursor(cursor)
    {
        registry.emplace_back(static_cast<T *>(this));
    }

private:
    void move_from(MDBGenCursor *src)
    {
        if (!d_registry) {
            return;
        }

        auto iter = std::find(d_registry->begin(), d_registry->end(), src);
        if (iter != d_registry->end()) {
            *iter = static_cast<T *>(this);
        } else {
            d_registry->emplace_back(static_cast<T *>(this));
        }
    }

public:
    MDBGenCursor(const MDBGenCursor &src) = delete;

    MDBGenCursor(MDBGenCursor &&src) noexcept
        : d_registry(src.d_registry)
        , d_cursor(src.d_cursor)
    {
        move_from(&src);
        src.d_registry = nullptr;
        src.d_cursor = nullptr;
    }

    MDBGenCursor &operator=(const MDBGenCursor &src) = delete;

    MDBGenCursor &operator=(MDBGenCursor &&src) noexcept
    {
        d_registry = src.d_registry;
        d_cursor = src.d_cursor;
        move_from(&src);
        src.d_registry = nullptr;
        src.d_cursor = nullptr;
        return *this;
    }

    ~MDBGenCursor()
    {
        close();
    }

public:
    int get(MDBOutVal &key, MDBOutVal &data, MDB_cursor_op op)
    {
        const auto rc = mdb_cursor_get(d_cursor, &key.d_mdbval, &data.d_mdbval, op);
        if (rc && rc != MDB_NOTFOUND)
            throw LMDBError("Unable to get from cursor: ", rc);
        return rc;
    }

    int find(const MDBInVal &in, MDBOutVal &key, MDBOutVal &data)
    {
        key.d_mdbval = in.d_mdbval;
        const auto rc = mdb_cursor_get(d_cursor, const_cast<MDB_val *>(&key.d_mdbval), &data.d_mdbval, MDB_SET);
        if (rc && rc != MDB_NOTFOUND)
            throw LMDBError("Unable to find from cursor: ", rc);
        return rc;
    }

    int lower_bound(const MDBInVal &in, MDBOutVal &key, MDBOutVal &data)
    {
        key.d_mdbval = in.d_mdbval;

        const auto rc = mdb_cursor_get(d_cursor, const_cast<MDB_val *>(&key.d_mdbval), &data.d_mdbval, MDB_SET_RANGE);
        if (rc && rc != MDB_NOTFOUND)
            throw LMDBError("Unable to lower_bound from cursor: ", rc);
        return rc;
    }

    int nextprev(MDBOutVal &key, MDBOutVal &data, MDB_cursor_op op)
    {
        const auto rc = mdb_cursor_get(d_cursor, &key.d_mdbval, &data.d_mdbval, op);
        if (rc && rc != MDB_NOTFOUND)
            throw LMDBError("Unable to prevnext from cursor: ", rc);
        return rc;
    }

    int next(MDBOutVal &key, MDBOutVal &data)
    {
        return nextprev(key, data, MDB_NEXT);
    }

    int prev(MDBOutVal &key, MDBOutVal &data)
    {
        return nextprev(key, data, MDB_PREV);
    }

    int currentlast(MDBOutVal &key, MDBOutVal &data, MDB_cursor_op op)
    {
        const auto rc = mdb_cursor_get(d_cursor, &key.d_mdbval, &data.d_mdbval, op);
        if (rc && rc != MDB_NOTFOUND)
            throw LMDBError("Unable to next from cursor: ", rc);
        return rc;
    }

    int current(MDBOutVal &key, MDBOutVal &data)
    {
        return currentlast(key, data, MDB_GET_CURRENT);
    }
    int last(MDBOutVal &key, MDBOutVal &data)
    {
        return currentlast(key, data, MDB_LAST);
    }
    int first(MDBOutVal &key, MDBOutVal &data)
    {
        return currentlast(key, data, MDB_FIRST);
    }

    operator MDB_cursor *()
    {
        return d_cursor;
    }

    operator bool() const
    {
        return d_cursor;
    }

    void close()
    {
        if (d_registry) {
            auto iter = std::find(d_registry->begin(), d_registry->end(), static_cast<T *>(this));
            if (iter != d_registry->end()) {
                d_registry->erase(iter);
            }
            d_registry = nullptr;
        }
        if (d_cursor) {
            mdb_cursor_close(d_cursor);
            d_cursor = nullptr;
        }
    }
};

/*!
 * \brief The MDBROCursor class represents a read-only cursor.
 */
class LMDB_SAFE_EXPORT MDBROCursor : public MDBGenCursor<MDBROTransactionImpl, MDBROCursor> {
public:
    MDBROCursor() = default;
    using MDBGenCursor<MDBROTransactionImpl, MDBROCursor>::MDBGenCursor;
    MDBROCursor(const MDBROCursor &src) = delete;
    MDBROCursor(MDBROCursor &&src) = default;
    MDBROCursor &operator=(const MDBROCursor &src) = delete;
    MDBROCursor &operator=(MDBROCursor &&src) = default;
    ~MDBROCursor() = default;
};

class MDBRWCursor;

/*!
 * \brief The MDBRWTransactionImpl class wraps write operations.
 */
class LMDB_SAFE_EXPORT MDBRWTransactionImpl : public MDBROTransactionImpl {
protected:
    MDBRWTransactionImpl(MDBEnv *parent, MDB_txn *txn);

private:
    static MDB_txn *openRWTransaction(MDBEnv *env, MDB_txn *parent, unsigned int flags);

private:
    std::vector<MDBRWCursor *> d_rw_cursors;

    void closeRWCursors();
    inline void closeRORWCursors()
    {
        closeROCursors();
        closeRWCursors();
    }

public:
    explicit MDBRWTransactionImpl(MDBEnv *parent, unsigned int flags = 0);

    MDBRWTransactionImpl(const MDBRWTransactionImpl &rhs) = delete;
    MDBRWTransactionImpl(MDBRWTransactionImpl &&rhs) = delete;
    MDBRWTransactionImpl &operator=(const MDBRWTransactionImpl &rhs) = delete;
    MDBRWTransactionImpl &operator=(MDBRWTransactionImpl &&rhs) = delete;

    ~MDBRWTransactionImpl() override;

    void commit() override;
    void abort() override;

    void clear(MDB_dbi dbi);

    void put(MDB_dbi dbi, const MDBInVal &key, const MDBInVal &val, unsigned int flags = 0)
    {
        if (!d_txn)
            throw LMDBError("Attempt to use a closed RW transaction for put");
        if (const auto rc = mdb_put(d_txn, dbi, const_cast<MDB_val *>(&key.d_mdbval), const_cast<MDB_val *>(&val.d_mdbval), flags))
            throw LMDBError("Putting data: ", rc);
    }

    int del(MDBDbi &dbi, const MDBInVal &key, const MDBInVal &val)
    {
        const auto rc = mdb_del(d_txn, dbi, const_cast<MDB_val *>(&key.d_mdbval), const_cast<MDB_val *>(&val.d_mdbval));
        if (rc && rc != MDB_NOTFOUND)
            throw LMDBError("Deleting data: ", rc);
        return rc;
    }

    int del(MDBDbi &dbi, const MDBInVal &key)
    {
        const auto rc = mdb_del(d_txn, dbi, const_cast<MDB_val *>(&key.d_mdbval), 0);
        if (rc && rc != MDB_NOTFOUND)
            throw LMDBError("Deleting data: ", rc);
        return rc;
    }

    int get(MDBDbi &dbi, const MDBInVal &key, MDBOutVal &val)
    {
        if (!d_txn)
            throw LMDBError("Attempt to use a closed RW transaction for get");

        const auto rc = mdb_get(d_txn, dbi, const_cast<MDB_val *>(&key.d_mdbval), &val.d_mdbval);
        if (rc && rc != MDB_NOTFOUND)
            throw LMDBError("Getting data: ", rc);
        return rc;
    }

    int get(MDBDbi &dbi, const MDBInVal &key, string_view &val)
    {
        MDBOutVal out;
        const auto rc = get(dbi, key, out);
        if (!rc)
            val = out.get<string_view>();
        return rc;
    }

    MDBDbi openDB(string_view dbname, unsigned int flags)
    {
        return MDBDbi(environment().d_env, d_txn, dbname, flags);
    }

    MDBRWCursor getRWCursor(const MDBDbi &);
    MDBRWCursor getCursor(const MDBDbi &);

    MDBRWTransaction getRWTransaction();
    MDBROTransaction getROTransaction();
};

/*!
 * \brief The MDBRWCursor class implements RW operations based on MDBGenCursor.
 * \remarks
 * - "A cursor in a write-transaction can be closed before its transaction ends, and will otherwise
 *   be closed when its transaction ends." This is a problem for us since it may means we are closing
 *   the cursor twice, which is bad.
 */
class LMDB_SAFE_EXPORT MDBRWCursor : public MDBGenCursor<MDBRWTransactionImpl, MDBRWCursor> {
public:
    MDBRWCursor() = default;
    using MDBGenCursor<MDBRWTransactionImpl, MDBRWCursor>::MDBGenCursor;
    MDBRWCursor(const MDBRWCursor &src) = delete;
    MDBRWCursor(MDBRWCursor &&src) = default;
    MDBRWCursor &operator=(const MDBRWCursor &src) = delete;
    MDBRWCursor &operator=(MDBRWCursor &&src) = default;
    ~MDBRWCursor() = default;

    void put(const MDBOutVal &key, const MDBInVal &data)
    {
        if (const auto rc = mdb_cursor_put(*this, const_cast<MDB_val *>(&key.d_mdbval), const_cast<MDB_val *>(&data.d_mdbval), MDB_CURRENT))
            throw LMDBError("Putting data via mdb_cursor_put: ", rc);
    }

    void put(const MDBOutVal &key, const MDBOutVal &data, unsigned int flags = 0)
    {
        if (const auto rc = mdb_cursor_put(*this, const_cast<MDB_val *>(&key.d_mdbval), const_cast<MDB_val *>(&data.d_mdbval), flags))
            throw LMDBError("Putting data via mdb_cursor_put: ", rc);
    }

    void del(unsigned int flags = 0)
    {
        if (const auto rc = mdb_cursor_del(*this, flags))
            throw LMDBError("Deleting data via mdb_cursor_del: ", rc);
    }
};

} // namespace LMDBSafe
