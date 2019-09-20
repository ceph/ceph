#include <iostream>
#include <sstream>
#include <iomanip>

#include <pthread.h>
#include <sqlite3.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <limits.h>


#include "include/rados/librados.hpp"
#include "include/radosstriper/libradosstriper.hpp"
#include "common/ceph_mutex.h"
#include "BufferCache.h"

#define CEPHSQLITE_USE_BUFFER_CACHE 1

using namespace libcephsqlite;

struct CephFile;
static int _cephsqlite3_Close(sqlite3_file *pFile);
static int _cephsqlite3_Read(sqlite3_file *pFile, void *zBuf, int iAmt, sqlite_int64 iOfst);
static int _cephsqlite3_Write(sqlite3_file *pFile, const void *zBuf, int iAmt, sqlite_int64 iOfst);
static int _cephsqlite3_Truncate(sqlite3_file *pFile, sqlite_int64 size);
static int _cephsqlite3_Sync(sqlite3_file *pFile, int flags);
static int _cephsqlite3_FileSize(sqlite3_file *pFile, sqlite_int64 *pSize);
static int _cephsqlite3_Lock(sqlite3_file *pFile, int eLock);
static int _cephsqlite3_Unlock(sqlite3_file *pFile, int eLock);
static int _cephsqlite3_CheckReservedLock(sqlite3_file *pFile, int *pResOut);
static int _cephsqlite3_FileControl(sqlite3_file *pFile, int op, void *pArg);
static int _cephsqlite3_SectorSize(sqlite3_file *pFile);
static int _cephsqlite3_DeviceCharacteristics(sqlite3_file *pFile);

static int _cephsqlite3_Open(sqlite3_vfs *pVfs, const char *zName, sqlite3_file *pFile, int flags, int *pOutFlags);
static int _cephsqlite3_Delete(sqlite3_vfs *pVfs, const char *zPath, int dirSync);
static int _cephsqlite3_Access(sqlite3_vfs *pVfs, const char *zPath, int flags, int *pResOut);
static int _cephsqlite3_FullPathname(sqlite3_vfs *pVfs, const char *zPath, int nPathOut, char *zPathOut);
static void *_cephsqlite3_DlOpen(sqlite3_vfs *pVfs, const char *zPath);
static void _cephsqlite3_DlError(sqlite3_vfs *pVfs, int nByte, char *zErrMsg);
static void (*_cephsqlite3_DlSym(sqlite3_vfs *pVfs, void *pH, const char *z))(void);
static void _cephsqlite3_DlClose(sqlite3_vfs *pVfs, void *pHandle);
static int _cephsqlite3_Randomness(sqlite3_vfs *pVfs, int nByte, char *zByte);
static int _cephsqlite3_Sleep(sqlite3_vfs *pVfs, int nMicro);
static int _cephsqlite3_CurrentTime(sqlite3_vfs *pVfs, double *pTime);

static sqlite3_vfs *_cephsqlite3__vfs(void);
static librados::IoCtx *get_io_ctx(const char *zPath);
static libradosstriper::RadosStriper *get_radosstriper(const char *db_name);
static std::string get_db_name(const char *zName);
static int get_lock_type(const std::string &db_name, const std::string &file_name);
static void set_lock_type(const std::string &db_name, const std::string &file_name, int eLock);
static std::string get_lock_file_name(const std::string &db_name, int eLock);
static int create_lock_files(librados::IoCtx *io_ctx, const std::string &db_name, bool must_create);
static int remove_lock_files(librados::IoCtx *io_ctx, const std::string &db_file);
static int lock_file_in_rados(librados::IoCtx *io_ctx, const std::string &lock_file_name, const std::string &lock_name, bool exclusive);
static void set_buffer_cache_locked(const char *file_name, libcephsqlite::BufferCache *bc);
static void set_buffer_cache(const char *file_name, libcephsqlite::BufferCache *bc);
static void remove_buffer_cache(const char *file_name);
//static std::string get_mapping_name(const char *file_name, const char *rados_namespace);
static libcephsqlite::BufferCache *get_buffer_cache(const char *file_name);
static libcephsqlite::BufferCache *get_buffer_cache_locked(const char *file_name);
static int buffer_cache_write(const char *file_name, char *buf, int iAmt, sqlite_int64 iOfst, bool dirty, bool &merged_dirty_range);
static int buffer_cache_read(const char *file_name, char *buf, int iAmt, sqlite_int64 iOfst);
static const char *get_rados_namespace(const char *uri_file_name);

enum eInitState {
  UNKNOWN       = 0,
  INITIALIZING  = 1,
  INITIALIZED   = 2
};

/* TODO add ref counting to ceph vfs context to help with multi-threaded
 * scenarios
 */
struct CephVFSContext {
  eInitState                    state     = eInitState::UNKNOWN;
  std::string                   rados_namespace;
  sqlite3                       *db       = nullptr;
  int                           page_size = -1; /* db PRAGMA page_size */
  librados::IoCtx               *io_ctx   = nullptr;
  libradosstriper::RadosStriper *rs       = nullptr;
  /* file name -> lock type map (SQLITE_LOCK_NONE, etc.)
   * file names include main as well as temporary files: eg. *-journal, *-wal, etc.)
   */
  std::map<std::string, int>    lock_info;

  // cache_map: filename -> buffer cache object
  std::map<std::string, libcephsqlite::BufferCache *> db_cache_map;

  ~CephVFSContext()
  {
    for (auto it = db_cache_map.begin(); it != db_cache_map.end(); ++it) {
      delete it->second;
      db_cache_map.erase(it);
    }
  }
};

struct CephFile {
  sqlite3_file  base;
  const char   *name                = nullptr;
  const char   *rados_namespace     = nullptr;
  int           file_open_flags     = 0;
  long          size                = 0;
  bool          dirty               = false;
};

// map to hold pointers to vfs contexts for various databases
static ceph::mutex                             vfs_context_map_mutex = ceph::make_mutex("vfs_context_map_mutex");
static std::map<std::string, CephVFSContext*>  vfs_context_map;

static const std::string lock_name = "cephsqlite3_vfs_lock";
static const std::string emptystr = "";


/*
** Close a file.
*/
static
int _cephsqlite3_Close(sqlite3_file *pFile)
{
  CephFile *p = (CephFile*)pFile;

  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << p->name << std::endl;
#if CEPHSQLITE_USE_BUFFER_CACHE
  libcephsqlite::BufferCache *bc = get_buffer_cache(p->name);
  if (bc) {
    remove_buffer_cache(p->name);
    delete bc; // delete the buffer cache; dtor forces shutdown
  }
#endif
  // if only we are closing the main database
  if (get_db_name(p->name) == p->name) {
    CephVFSContext *cc = nullptr;

    vfs_context_map_mutex.lock();
    cc = vfs_context_map[std::string(p->name)];
    if (cc)
      vfs_context_map.erase(std::string(p->name));
    vfs_context_map_mutex.unlock();

    if (cc) {
      delete cc->rs;
      if (cc->io_ctx) {
        cc->io_ctx->close();
        delete cc->io_ctx;
      }
      delete cc;
    }
  }

  return SQLITE_OK;
}

/*
** Read data from a file.
*/
static
int _cephsqlite3_Read(sqlite3_file *pFile, void *zBuf, int iAmt, sqlite_int64 iOfst)
{
  CephFile *p = (CephFile*)pFile;
  char *b = static_cast<char*>(zBuf);
  bool merged_dirty_range = false;

  // std::cerr << std::endl;
  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "read(" << p->name << ", " << std::dec << iAmt << ", " << std::dec << iOfst << ")" << std::endl;
#if CEPHSQLITE_USE_BUFFER_CACHE
  if (buffer_cache_read(p->name, b, iAmt, iOfst) == SQLITE_OK) {
    return SQLITE_OK;
  } else {
    // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "buffer_cache_read(" << std::dec << iAmt << ", " << iOfst << ") failed!" << std::endl;
    // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << "-----" << std::endl;
  }
#endif
  libradosstriper::RadosStriper *rs = get_radosstriper(get_db_name(p->name).c_str());

  /* ceph::bufferlist buffer pointers are all char* */
  if (rs->read(p->name, b, iAmt, iOfst) < 0) {
    // int e = errno;
    // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << "::errno:" << e << std::endl;
    return SQLITE_IOERR_READ;
  }
#if CEPHSQLITE_USE_BUFFER_CACHE
  if (buffer_cache_write(p->name, b, iAmt, iOfst, false, merged_dirty_range) != SQLITE_OK) {
    //return SQLITE_IOERR;
    // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << std::dec << __LINE__ << "::buffer_cache_insert failed!" << std::endl;
  } else {
    // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "buffer_cache_insert(" << std::dec << iAmt << ", " << iOfst << ") done" << std::endl;
  }
  // this second lookup is to return merged page in case the page cache has
  // dirty patches
  if (merged_dirty_range) {
    if (buffer_cache_read(p->name, b, iAmt, iOfst) == SQLITE_OK) {
      return SQLITE_OK;
    } else {
      // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "buffer_cache_read(" << std::dec << iAmt << ", " << iOfst << ") failed!" << std::endl;
      // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << "-----" << std::endl;
      return SQLITE_IOERR_READ;
    }
  }
#endif
  return SQLITE_OK;
}

/*
** Write data to a crash-file.
*/
static
int _cephsqlite3_Write(sqlite3_file *pFile, const void *zBuf, int iAmt, sqlite_int64 iOfst)
{
  CephFile *p = (CephFile*)pFile;
  bool merged_dirty_range = false;

  // std::cerr << std::endl;
  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "write(" << p->name << ", " << std::dec << iAmt << ", " << std::dec << iOfst << ")" << std::endl;
  /* ceph::bufferlist buffer pointers are all char* */
  char *b = reinterpret_cast<char*>(const_cast<void*>(zBuf));
#if CEPHSQLITE_USE_BUFFER_CACHE
  if (buffer_cache_write(p->name, b, iAmt, iOfst, true, merged_dirty_range) != SQLITE_OK) {
    // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "buffer_cache_insert failed!" << std::endl;
    //return SQLITE_IOERR;
  } else {
    // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "buffer_cache_insert(" << std::dec << iAmt << ", " << iOfst << ") done" << std::endl;
    // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << "-----" << std::endl;
    p->dirty = true;
    if ((iOfst + iAmt) > p->size)
      p->size = (iOfst + iAmt);

    return SQLITE_OK;
  }
#endif

  libradosstriper::RadosStriper *rs = get_radosstriper(get_db_name(p->name).c_str());

  ceph::bufferlist bl = ceph::bufferlist::static_from_mem(b, iAmt);

  if (rs->write(p->name, bl, iAmt, iOfst) < 0)
    return SQLITE_IOERR;

  return SQLITE_OK;

}

/*
** Truncate a file. This is a no-op for this VFS (see header comments at
** the top of the file).
*/
static
int _cephsqlite3_Truncate(sqlite3_file *pFile, sqlite_int64 size)
{
  CephFile *p = (CephFile*)pFile;

  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << p->name << std::endl;
  
  libradosstriper::RadosStriper *rs = get_radosstriper(get_db_name(p->name).c_str());

  if (rs->trunc(p->name, size) != 0)
    return SQLITE_IOERR;

  p->size = size;
  p->dirty = (p->size != 0);

  return SQLITE_OK;
}

/*
** Sync the contents of the file to the persistent media.
*/
static
int _cephsqlite3_Sync(sqlite3_file *pFile, int flags)
{
  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << std::endl;
  CephFile *p = (CephFile*)pFile;
#if CEPHSQLITE_USE_BUFFER_CACHE
  libcephsqlite::BufferCache *bc = get_buffer_cache(p->name);

  bc->lock();
  while (bc->is_dirty()) {
    flush_pages(bc, false);
  }
  bc->unlock();
#endif
  return SQLITE_OK;
}


/*
** Write the size of the file in bytes to *pSize.
*/
static
int _cephsqlite3_FileSize(sqlite3_file *pFile, sqlite_int64 *pSize)
{
  CephFile *p = (CephFile*)pFile;

  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << p->name << std::endl;
  
  libradosstriper::RadosStriper *rs = get_radosstriper(get_db_name(p->name).c_str());

  uint64_t  size = 0;
  time_t    mtime = 0;
  int       rc = rs->stat(p->name, &size, &mtime);

#if CEPHSQLITE_USE_BUFFER_CACHE
  if (p->dirty)
    size = p->size;
  else
    p->size = size;
#endif

  *pSize = 0;

  if (rc == 0)
    *pSize = (sqlite_int64)size;

  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << *pSize << std::endl;
  return SQLITE_OK;
}

#define CASE(x) case x: return #x

static
const char *lock_type_str(int eLock)
{
  switch (eLock) {
    CASE(SQLITE_LOCK_NONE);
    CASE(SQLITE_LOCK_SHARED);
    CASE(SQLITE_LOCK_RESERVED);
    CASE(SQLITE_LOCK_PENDING);
    CASE(SQLITE_LOCK_EXCLUSIVE);
  }
  return "UNKNOWN";
}

static
std::string get_lock_file_name(const std::string &db_name, int eLock)
{
  std::string lock_file_name = db_name;
  lock_file_name += "-";
  lock_file_name += lock_type_str(eLock);
  return lock_file_name;
}

std::string get_cookie()
{
  std::stringstream ss;

  ss << std::hex << std::setfill('0') << std::setw(16) << "0x" << pthread_self();

  return ss.str();
}

/*
** Locking functions. The xLock() and xUnlock() methods are both no-ops.
** The xCheckReservedLock() always indicates that no other process holds
** a reserved lock on the database file. This ensures that if a hot-journal
** file is found in the file-system it is rolled back.
*/
static
int _cephsqlite3_Lock(sqlite3_file *pFile, int eLock)
{
  CephFile *p = (CephFile *)pFile;

  assert(p != NULL);

  std::string db_name = get_db_name(p->name);
  std::string db_file = p->name;
  /* Make sure the locking sequence is correct.
  **  (1) We never move from unlocked to anything higher than shared lock.
  **  (2) SQLite never explicitly requests a pendig lock.
  **  (3) A shared lock is always held when a reserve lock is requested.
  */
  int curr_lock_type = get_lock_type(db_name, db_file);

  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << "::eLock " << p->name << " from:" << lock_type_str(curr_lock_type) << " to:" << lock_type_str(eLock) << std::endl;

  assert(curr_lock_type != SQLITE_LOCK_NONE || eLock == SQLITE_LOCK_SHARED);
  assert(eLock != SQLITE_LOCK_PENDING);
  assert(eLock != SQLITE_LOCK_RESERVED || curr_lock_type == SQLITE_LOCK_SHARED);


  if (curr_lock_type == eLock)
      return SQLITE_OK;

  librados::IoCtx *io_ctx = get_io_ctx(db_name.c_str());
  assert(io_ctx != nullptr);

  std::string lock_file_name;

  if (curr_lock_type == SQLITE_LOCK_NONE) {
    if (eLock == SQLITE_LOCK_SHARED) {
      lock_file_name = get_lock_file_name(db_file, SQLITE_LOCK_SHARED);
      if (lock_file_in_rados(io_ctx, lock_file_name, lock_name, false) != 0)
        return SQLITE_BUSY;
      set_lock_type(db_name, db_file, SQLITE_LOCK_SHARED);
      return SQLITE_OK;
    }
    return SQLITE_IOERR_RDLOCK;
  } else if (curr_lock_type == SQLITE_LOCK_SHARED) {
    lock_file_name = get_lock_file_name(db_file, SQLITE_LOCK_SHARED /*SQLITE_LOCK_EXCLUSIVE*/);

    // unlock in shared mode and lock in exclusive mode
    if (io_ctx->unlock(lock_file_name, lock_name, get_cookie()) == 0) {
      if (lock_file_in_rados(io_ctx, lock_file_name, lock_name, true) != 0) {
        // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << "::returning SQLITE_BUSY" << std::endl;
        return SQLITE_BUSY;
      }
      set_lock_type(db_name, db_file, eLock);
      return SQLITE_OK;
    }
  } else if (eLock == SQLITE_LOCK_RESERVED || eLock == SQLITE_LOCK_PENDING || eLock == SQLITE_LOCK_EXCLUSIVE) {
    set_lock_type(db_name, db_file, eLock);
    return SQLITE_OK;
  }

  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << "::returning SQLITE_IOERR_LOCK" << std::endl;
  return SQLITE_IOERR_LOCK;
}

static
int _cephsqlite3_Unlock(sqlite3_file *pFile, int eLock)
{
  CephFile *p = (CephFile *)pFile;

  assert(p != NULL);


  std::string db_name = get_db_name(p->name);
  std::string db_file = p->name;

  librados::IoCtx *io_ctx = get_io_ctx(db_name.c_str());
  assert(io_ctx != nullptr);

  int curr_lock_type = get_lock_type(db_name, db_file);

  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << "::eLock " << p->name << " from:" << lock_type_str(curr_lock_type) << " to:" << lock_type_str(eLock) << std::endl;

  if (eLock == curr_lock_type)
    return SQLITE_OK;

  assert(eLock < curr_lock_type);

  std::string lock_file_name = get_lock_file_name(db_file, SQLITE_LOCK_SHARED);
  if (eLock <= SQLITE_LOCK_SHARED) {
    if (io_ctx->unlock(lock_file_name, lock_name, get_cookie()) != 0)
      return SQLITE_IOERR_UNLOCK;
  }

  if (eLock == SQLITE_LOCK_SHARED) {
    if (lock_file_in_rados(io_ctx, lock_file_name, lock_name, false) != 0)
      return SQLITE_BUSY;
  }

  set_lock_type(db_name, db_file, eLock);

  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":: returning SQLITE_OK" << std::endl;
  return SQLITE_OK;
}

static
int _cephsqlite3_CheckReservedLock(sqlite3_file *pFile, int *pResOut)
{
  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << std::endl;
  CephFile *p = (CephFile *)pFile;

  assert(p != NULL);

  std::string db_name = get_db_name(p->name);
  std::string db_file = p->name;
  // librados::IoCtx *io_ctx = get_io_ctx(db_name.c_str());
  // assert(io_ctx != nullptr);
  int curr_lock_type = get_lock_type(db_name, db_file);

  *pResOut = (curr_lock_type > SQLITE_LOCK_SHARED);
  return SQLITE_OK;
}

/*
** No xFileControl() verbs are implemented by this VFS.
*/
static
int _cephsqlite3_FileControl(sqlite3_file *pFile, int op, void *pArg)
{
  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << std::endl;
  return SQLITE_NOTFOUND;
}

/*
** The xSectorSize() and xDeviceCharacteristics() methods. These two
** may return special values allowing SQLite to optimize file-system
** access to some extent. But it is also safe to simply return 0.
*/
#define CEPHSQLITE_SECTOR_SIZE (8<<10)
static
int _cephsqlite3_SectorSize(sqlite3_file *pFile)
{
  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << p->name << ":" << CEPHSQLITE_SECTOR_SIZE << std::endl;
  return CEPHSQLITE_SECTOR_SIZE;
}

static
int _cephsqlite3_DeviceCharacteristics(sqlite3_file *pFile)
{
  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << std::endl;
  return 0;
}

/*
** Open a file handle.
*/
static
int _cephsqlite3_Open(
  sqlite3_vfs *pVfs,              /* VFS */
  const char *zName,              /* File to open, or 0 for a temp file */
  sqlite3_file *pFile,            /* Pointer to DemoFile struct to populate */
  int flags,                      /* Input SQLITE_OPEN_XXX flags */
  int *pOutFlags                  /* Output SQLITE_OPEN_XXX flags (or NULL) */
)
{
  static const sqlite3_io_methods _cephsqlite3_io = {
    1,                                     /* iVersion */
    _cephsqlite3_Close,                    /* xClose */
    _cephsqlite3_Read,                     /* xRead */
    _cephsqlite3_Write,                    /* xWrite */
    _cephsqlite3_Truncate,                 /* xTruncate */
    _cephsqlite3_Sync,                     /* xSync */
    _cephsqlite3_FileSize,                 /* xFileSize */
    _cephsqlite3_Lock,                     /* xLock */
    _cephsqlite3_Unlock,                   /* xUnlock */
    _cephsqlite3_CheckReservedLock,        /* xCheckReservedLock */
    _cephsqlite3_FileControl,              /* xFileControl */
    _cephsqlite3_SectorSize,               /* xSectorSize */
    _cephsqlite3_DeviceCharacteristics     /* xDeviceCharacteristics */
  };

  CephFile *p = (CephFile*)pFile; /* Populate this structure */

  if ((zName == 0) || (strncmp(zName, ":memory:", 8) == 0)) {
    /* we are not going to create temporary files */
    return SQLITE_IOERR;
  }

  p->name = zName; /* save the file name */
  p->file_open_flags = flags;
  libradosstriper::RadosStriper* rs = get_radosstriper(get_db_name(zName).c_str());

  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "zName:" << zName << ", db name:" << get_db_name(zName) << ", namespace:" << p->rados_namespace << std::endl;

  // std::cerr << "db name:" << get_db_name(zName) << std::endl;
  // std::cerr << "rs:" << std::hex << rs << std::endl;

  ceph_assert(rs != nullptr);

  /* exclusive create
   * radosstriper doesn't have a create()! ... since the data is striped,
   * radosstriper can't decide beforehand which of the stripe files it needs
   * to create
   * so here, we just force create the first stripe by writing to offset zero
   * so that the rest of the interface functions work as expected
   */
  if (flags & SQLITE_OPEN_CREATE) {
    // std::cerr << __FUNCTION__ << "::creating database" << std::endl;
    char *dummy = new char[libcephsqlite::PAGE_SIZE];
    ceph_assert(dummy != nullptr);
    ceph::bufferlist bl = ceph::bufferlist::static_from_mem(dummy, libcephsqlite::PAGE_SIZE);
    if (rs->write(p->name, bl, libcephsqlite::PAGE_SIZE, 0) != 0) {
      // int e = errno;
      // std::cerr << __FUNCTION__ << "::error: during write():errno(" << e << ")" << std::endl;
      delete [] dummy;
      return SQLITE_IOERR_WRITE;
    }
    delete [] dummy;
    if (rs->trunc(p->name, 0) < 0) {
      // int e = errno;
      // std::cerr << __FUNCTION__ << "::error: during trunc():errno(" << e << ")" << std::endl;
      return SQLITE_IOERR_TRUNCATE;
    }

    // we also create the sentinel file which would be for locking operations
    librados::IoCtx *io_ctx = get_io_ctx(get_db_name(zName).c_str());

    if (create_lock_files(io_ctx, std::string(zName), true) != SQLITE_OK)
      return SQLITE_ERROR;

#if CEPHSQLITE_USE_BUFFER_CACHE
    /* We lock the context map mutex here so that the get and set buffer cache
     * operations remain atomic to other threads. Threads will be sharing the
     * buffer cache to the same database.
     */
    vfs_context_map_mutex.lock();
    if (get_buffer_cache_locked(p->name) == nullptr) {
      libcephsqlite::BufferCache *bc =
        new libcephsqlite::BufferCache(p->name, rs, (1<<10), 64<<10);
      set_buffer_cache_locked(p->name, bc);
      bc->start();
    }
    vfs_context_map_mutex.unlock();
#endif
  }
  if (pOutFlags) {
    *pOutFlags = flags;
  }
  p->base.pMethods = &_cephsqlite3_io;
  return SQLITE_OK;
}

/*
** Delete the file identified by argument zPath. If the dirSync parameter
** is non-zero, then ensure the file-system modification to delete the
** file has been synced to disk before returning.
*/
static
int _cephsqlite3_Delete(sqlite3_vfs *pVfs, const char *zPath, int dirSync)
{
  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << zPath << std::endl;
  libradosstriper::RadosStriper *rs = get_radosstriper(get_db_name(zPath).c_str());
  librados::IoCtx *io_ctx           = get_io_ctx(get_db_name(zPath).c_str());

  int ret = rs->remove(zPath);
  if (ret == 0) {
    // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << "::deleted " << zPath << std::endl;
    return SQLITE_OK;
  }

  remove_lock_files(io_ctx, std::string(zPath));

  // int e = errno;
  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << "::error deleting " << zPath << " ret:" << ret << ", errno:" << e << std::endl;
  return SQLITE_IOERR_DELETE;
}

/*
** Query the file-system to see if the named file exists, is readable or
** is both readable and writable.
*/
static
int _cephsqlite3_Access(sqlite3_vfs *pVfs, const char *zPath, int flags, int *pResOut)
{
  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << ":zPath:" << zPath << std::endl;

  ceph_assert(flags == SQLITE_ACCESS_EXISTS  ||       /* access(zPath, F_OK) */
              flags == SQLITE_ACCESS_READ    ||       /* access(zPath, R_OK) */
              flags == SQLITE_ACCESS_READWRITE        /* access(zPath, R_OK|W_OK) */
  );

  libradosstriper::RadosStriper *rs = get_radosstriper(get_db_name(zPath).c_str());

  uint64_t  size = 0;
  time_t    mtime = 0;
  int       rc = rs->stat(zPath, &size, &mtime);

  *pResOut = (rc == 0);

  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << *pResOut << std::endl;

  return SQLITE_OK;
}

/*
** Argument zPath points to a nul-terminated string containing a file path.
** If zPath is an absolute path, then it is copied as is into the output
** buffer. Otherwise, if it is a relative path, then the equivalent full
** path is written to the output buffer.
**
** This function assumes that paths are UNIX style. Specifically, that:
**
**   1. Path components are separated by a '/'. and
**   2. Full paths begin with a '/' character.
*/
static
int _cephsqlite3_FullPathname(
  sqlite3_vfs *pVfs,              /* VFS */
  const char *zPath,              /* Input path (possibly a relative path) */
  int nPathOut,                   /* Size of output buffer in bytes */
  char *zPathOut                  /* Pointer to output buffer */
)
{
  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << ":zPath:" << zPath << std::endl;

  /* There are no directories to be searched for RADOS obects.
   * They are always at the root.
   * So, just copy the path if starting with '/' or prefix the path with a '/'
   * and copy to output.
   */
  if (strlen(zPath) >= (unsigned long)nPathOut)
    return SQLITE_ERROR;

  memcpy(zPathOut, zPath, strlen(zPath) + 1);
  zPathOut[strlen(zPath)] = '\0';

  return SQLITE_OK;
}

/*
** The following four VFS methods:
**
**   xDlOpen
**   xDlError
**   xDlSym
**   xDlClose
**
** are supposed to implement the functionality needed by SQLite to load
** extensions compiled as shared objects. This VFS does not support
** this functionality, so the following functions are no-ops.
*/
static
void *_cephsqlite3_DlOpen(sqlite3_vfs *pVfs, const char *zPath)
{
  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << std::endl;
  return 0;
}

static
void _cephsqlite3_DlError(sqlite3_vfs *pVfs, int nByte, char *zErrMsg)
{
  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << std::endl;
  snprintf(zErrMsg, nByte, "Loadable extensions are not supported");
  zErrMsg[nByte-1] = '\0';
}

static
void (*_cephsqlite3_DlSym(sqlite3_vfs *pVfs, void *pH, const char *z))(void)
{
  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << std::endl;
  return 0;
}

static
void _cephsqlite3_DlClose(sqlite3_vfs *pVfs, void *pHandle)
{
  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << std::endl;
  return;
}

/*
** Parameter zByte points to a buffer nByte bytes in size. Populate this
** buffer with pseudo-random data.
*/
static
int _cephsqlite3_Randomness(sqlite3_vfs *pVfs, int nByte, char *zByte)
{
  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << std::endl;
  int fd = open("/dev/urandom", O_RDONLY);
  if (fd >= 0) {
    if (read(fd, zByte, nByte) == nByte) {
      close(fd);
      return SQLITE_OK;
    }
    close(fd);
  }
  return SQLITE_ERROR;
}

/*
** Sleep for at least nMicro microseconds. Return the (approximate) number
** of microseconds slept for.
*/
static
int _cephsqlite3_Sleep(sqlite3_vfs *pVfs, int nMicro)
{
  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << std::endl;
  sleep(nMicro / 1000000);
  usleep(nMicro % 1000000);
  return nMicro;
}

/*
** Set *pTime to the current UTC time expressed as a Julian day. Return
** SQLITE_OK if successful, or an error code otherwise.
**
**   http://en.wikipedia.org/wiki/Julian_day
**
** This implementation is not very good. The current time is rounded to
** an integer number of seconds. Also, assuming time_t is a signed 32-bit
** value, it will stop working some time in the year 2038 AD (the so-called
** "year 2038" problem that afflicts systems that store time this way).
*/
static
int _cephsqlite3_CurrentTime(sqlite3_vfs *pVfs, double *pTime)
{
  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << std::endl;
  time_t t = time(0);
  *pTime = t/86400.0 + 2440587.5;
  return SQLITE_OK;
}

static
sqlite3_vfs *_cephsqlite3__vfs(void)
{
  static sqlite3_vfs _cephsqlite3_vfs = {
    1,                               /* iVersion */
    sizeof(CephFile),                /* szOsFile */
    PATH_MAX,                        /* mxPathname */
    0,                               /* pNext */
    "cephsqlite3",                   /* zName */
    0,                               /* pAppData */
    _cephsqlite3_Open,               /* xOpen */
    _cephsqlite3_Delete,             /* xDelete */
    _cephsqlite3_Access,             /* xAccess */
    _cephsqlite3_FullPathname,       /* xFullPathname */
    _cephsqlite3_DlOpen,             /* xDlOpen */
    _cephsqlite3_DlError,            /* xDlError */
    _cephsqlite3_DlSym,              /* xDlSym */
    _cephsqlite3_DlClose,            /* xDlClose */
    _cephsqlite3_Randomness,         /* xRandomness */
    _cephsqlite3_Sleep,              /* xSleep */
    _cephsqlite3_CurrentTime,        /* xCurrentTime */
  };
  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << std::endl;
  return &_cephsqlite3_vfs;
}

static
void __attribute__ ((constructor)) _cephsqlite3__vfs_register()
{
  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << std::endl;
  sqlite3_vfs_register(_cephsqlite3__vfs(), 1);
}

static
void __attribute__ ((destructor)) _cephsqlite3__vfs_unregister()
{
  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << std::endl;
  sqlite3_vfs_unregister(_cephsqlite3__vfs());
}

extern "C"
sqlite3 *ceph_sqlite3_open(
  librados::Rados &cluster,
  const char *dbname,           /* eg. "sql" instead of "sql.db" */
  const char *rados_namespace,
  int ceph_pool_id,
  bool must_create
)
{
  sqlite3 *db = NULL;
  std::stringstream ss;

  /* since the same file name can be used to create databases in different
   * namespaces, we'd rather keep the namespace prefixed with the file name
   * to make the search easier
   */
  ss << dbname << ".db";
  std::string db_name_s(ss.str());

  vfs_context_map_mutex.lock();
  if (vfs_context_map[db_name_s] == nullptr) {
    // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << "::must_create:" << must_create << std::endl;
    int ret = -1;
    /* FIXME
     * how long should the io_ctx and cluster objects exist for radosstriper
     * to be functional ?
     */
    librados::IoCtx *io_ctx = new librados::IoCtx;
    libradosstriper::RadosStriper *rs = new libradosstriper::RadosStriper;

    ceph_assert(io_ctx);
    ceph_assert(rs);

    ret = cluster.ioctx_create2(ceph_pool_id, *io_ctx);
    if (ret < 0) {
      /* unable to create the IO Context */
      return NULL;
    }

    io_ctx->set_namespace(rados_namespace);

    ret = libradosstriper::RadosStriper::striper_create(*io_ctx, rs);
    if (ret < 0) {
      /* unable to create the striper */
      return NULL;
    }

    uint64_t alignment = 0;
    ret = io_ctx->pool_required_alignment2(&alignment);
    if (ret < 0) {
      /* no alignment retrieved */
      return NULL;
    }

    rs->set_object_layout_stripe_unit(alignment);

    const int db_open_flags = SQLITE_OPEN_NOMUTEX       | /* single client access */
                              SQLITE_OPEN_PRIVATECACHE  |
                              SQLITE_OPEN_READWRITE     |
                              SQLITE_OPEN_URI           |
                              (must_create ? SQLITE_OPEN_CREATE : 0);


    /* pass the address of the RadosStriper C++ object in the URI so that the VFS
     * methods can access it
     */
    std::string mode = (must_create ? "rwc" : "rw");

    /* we are resuing the stringstream; so clear the contents and flags */
    ss.str("");
    ss.clear();

    /* create a URI based file name */
    ss << "file:" << db_name_s << "?mode=" << mode << "&cache=shared&vfs=cephsqlite3";
    std::string uri_filename(ss.str());

    CephVFSContext *cc = NULL;

    cc = new CephVFSContext;
    ceph_assert(cc);
    cc->rados_namespace = rados_namespace;
    cc->io_ctx = io_ctx;
    cc->rs     = rs;
    cc->state  = eInitState::INITIALIZING;
    vfs_context_map[db_name_s] = cc;

    vfs_context_map_mutex.unlock();

    /* we can't call sqlite3_open_v2() with the vfs context map mutex held
     * so we need to wait until the call to sqlite3_open_v2() returns with
     * the cc->db parameter initilized; its only then that the vfs construction
     * can be declared as complete
     */
    ret = sqlite3_open_v2(uri_filename.c_str(), &(cc->db), db_open_flags, "cephsqlite3");
    if (ret < 0) {
      /* db creation failed */
      vfs_context_map_mutex.lock();
      vfs_context_map.erase(db_name_s);
      vfs_context_map_mutex.unlock();
      delete cc;
      delete rs;
      delete io_ctx;

      return NULL;
    }
    vfs_context_map_mutex.lock();
    cc->state  = eInitState::INITIALIZED;
    vfs_context_map_mutex.unlock();
    db = cc->db;
  } else {
    while (vfs_context_map[db_name_s] &&
           (vfs_context_map[db_name_s]->state != eInitState::INITIALIZED)) {
        vfs_context_map_mutex.unlock();
        sleep(1);
        vfs_context_map_mutex.lock();
    }

    if (!vfs_context_map[db_name_s]) {
      /* most likely sqlite3_open_v2() called failed */
      db = NULL;
    } else {
      db = vfs_context_map[db_name_s]->db;
    }
    vfs_context_map_mutex.unlock();
  }

  return db;
}

extern "C"
void ceph_sqlite3_set_db_params(
  const char *dbname,           /* eg. "sql" instead of "sql.db" */
  int stripe_count,
  int obj_size
)
{
  vfs_context_map_mutex.lock();
  CephVFSContext *cc = vfs_context_map[std::string(dbname) + ".db"];

  if (cc) {
    libradosstriper::RadosStriper *rs = cc->rs;

    if (rs) {
      rs->set_object_layout_stripe_count(stripe_count);
      rs->set_object_layout_object_size(obj_size);
    }
  }
  vfs_context_map_mutex.unlock();
}

extern "C"
void ceph_sqlite3_set_db_page_size(sqlite3 *db, int page_size)
{
  char *sErrMsg = NULL;
  std::stringstream ss;

  ss << "PRAGMA page_size = " << page_size;
  if (sqlite3_exec(db, ss.str().c_str(), NULL, NULL, &sErrMsg) == SQLITE_OK) {
    const char *db_name = sqlite3_db_filename(db, "main");

    ceph_assert(db_name);

    vfs_context_map_mutex.lock();
    CephVFSContext *cc = vfs_context_map[db_name];

    cc->page_size = page_size;
    vfs_context_map_mutex.unlock();
  }
}

int _ceph_sqlite3_get_db_page_size_callback(
  void *arg,
  int cols,
  char **col_names,
  char **col_values)
{
  ceph_assert(arg);
  ceph_assert(cols == 1);

  vfs_context_map_mutex.lock();
  CephVFSContext *cc = reinterpret_cast<CephVFSContext *>(arg);

  cc->page_size = atoi(col_values[0]);
  vfs_context_map_mutex.unlock();
  return 0;
}

int ceph_sqlite3_get_db_page_size(sqlite3 *db)
{
  vfs_context_map_mutex.lock();
  CephVFSContext *cc = vfs_context_map[std::string(sqlite3_db_filename(db, "main"))];
  vfs_context_map_mutex.unlock();

  if (cc->page_size != -1)
    return cc->page_size;

  char *sErrMsg = NULL;
  std::stringstream ss;

  if (sqlite3_exec(db, "PRAGMA page_size", _ceph_sqlite3_get_db_page_size_callback, cc, &sErrMsg) == SQLITE_OK) {
    // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << "::error: while getting db page_size: " << sErrMsg << std::endl;
    return -1;
  }
  return cc->page_size;
}

static
int create_lock_files(librados::IoCtx *io_ctx, const std::string &db_file, bool must_create)
{
  int ret = io_ctx->create(get_lock_file_name(db_file, SQLITE_LOCK_SHARED), must_create);
  if (ret < 0 && ret != -EEXIST) {
    // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << "::error: while creating:" << get_lock_file_name(db_file, SQLITE_LOCK_SHARED) << std::endl;
    return SQLITE_ERROR;
  }
#if 0
  ret = io_ctx->create(get_lock_file_name(db_file, SQLITE_LOCK_RESERVED), must_create);
  if (ret < 0 && ret != -EEXIST) {
    // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << "::error: while creating:" << get_lock_file_name(db_file, SQLITE_LOCK_RESERVED) << std::endl;
    return SQLITE_ERROR;
  }

  ret = io_ctx->create(get_lock_file_name(db_file, SQLITE_LOCK_PENDING), must_create);
  if (ret < 0 && ret != -EEXIST) {
    // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << "::error: while creating:" << get_lock_file_name(db_file, SQLITE_LOCK_PENDING) << std::endl;
    return SQLITE_ERROR;
  }

  ret = io_ctx->create(get_lock_file_name(db_file, SQLITE_LOCK_EXCLUSIVE), must_create);
  if (ret < 0 && ret != -EEXIST) {
    // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << "::error: while creating:" << get_lock_file_name(db_file, SQLITE_LOCK_EXCLUSIVE) << std::endl;
    return SQLITE_ERROR;
  }
#endif
  return SQLITE_OK;
}

static
int remove_lock_files(librados::IoCtx *io_ctx, const std::string &db_file)
{
  io_ctx->remove(get_lock_file_name(db_file, SQLITE_LOCK_SHARED));
  io_ctx->remove(get_lock_file_name(db_file, SQLITE_LOCK_RESERVED));
  io_ctx->remove(get_lock_file_name(db_file, SQLITE_LOCK_PENDING));
  io_ctx->remove(get_lock_file_name(db_file, SQLITE_LOCK_EXCLUSIVE));

  return SQLITE_OK;
}

/* input is the full URI file name*/
static
librados::IoCtx *get_io_ctx(const char *zPath)
{
  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "zPath:" << zPath << std::endl;

  librados::IoCtx *io_ctx = nullptr;

  vfs_context_map_mutex.lock();
  if (vfs_context_map[std::string(zPath)])
    io_ctx = vfs_context_map[std::string(zPath)]->io_ctx;
  vfs_context_map_mutex.unlock();

  return io_ctx;
}

/* input is the full URI file name*/
static
libradosstriper::RadosStriper *get_radosstriper(const char *db_name)
{
  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "zPath:" << zPath << std::endl;

  libradosstriper::RadosStriper *rs = nullptr;

  vfs_context_map_mutex.lock();
  if (vfs_context_map[std::string(db_name)])
    rs = vfs_context_map[std::string(db_name)]->rs;
  vfs_context_map_mutex.unlock();

  return rs;
}

static
std::string get_db_name(const char *zName)
{
  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << ":" << std::dec << __LINE__ << "::" << "zName:" << zName << std::endl;
  /* eg. "accounts.db-journal"
   * so filename should be extracted as "accounts.db"
   * NOTE main database file name will always have a .db extension
   *      see ceph_sqlite3_open()
   */
  const char *e = strstr(zName, ".db-");
  if (!e)
      return std::string(zName);

  e += 3; // point to the '-' after .db

  return std::string(zName, e - zName);
}

static
int get_lock_type(const std::string &db_name, const std::string &file_name)
{
  int eLock = SQLITE_LOCK_NONE;

  vfs_context_map_mutex.lock();
  CephVFSContext *cc = vfs_context_map[db_name];
  ceph_assert(cc);

  eLock = cc->lock_info[file_name];
  vfs_context_map_mutex.unlock();

  return eLock;
}

static
void set_buffer_cache_locked(const char *file_name, libcephsqlite::BufferCache *bc)
{
  CephVFSContext *cc = vfs_context_map[get_db_name(file_name)];
  ceph_assert(cc != NULL);

  cc->db_cache_map[std::string(file_name)] = bc;
}

static
void set_buffer_cache(const char *file_name, libcephsqlite::BufferCache *bc)
{
  vfs_context_map_mutex.lock();
  CephVFSContext *cc = vfs_context_map[get_db_name(file_name)];
  ceph_assert(cc != NULL);

  cc->db_cache_map[std::string(file_name)] = bc;
  vfs_context_map_mutex.unlock();
}

static
void remove_buffer_cache(const char *file_name)
{
  vfs_context_map_mutex.lock();
  CephVFSContext *cc = vfs_context_map[get_db_name(file_name)];
  ceph_assert(cc != NULL);

  cc->db_cache_map.erase(std::string(file_name));
  vfs_context_map_mutex.unlock();
}

#if 0
static
std::string get_mapping_name(const char *file_name, const char *rados_namespace)
{
  std::string mapping_name = rados_namespace;

  mapping_name += "::";
  mapping_name += get_db_name(file_name);
  return mapping_name;
}
#endif

/* IMPORTANT
 * Please lock vfs_context_map_mutex before calling this function */
static
libcephsqlite::BufferCache *get_buffer_cache_locked(const char *file_name)
{
  CephVFSContext *cc = vfs_context_map[get_db_name(file_name)];
  ceph_assert(cc);

  return cc->db_cache_map[std::string(file_name)];
}

static
libcephsqlite::BufferCache *get_buffer_cache(const char *file_name)
{
  libcephsqlite::BufferCache *bc = nullptr;

  vfs_context_map_mutex.lock();
  CephVFSContext *cc = vfs_context_map[get_db_name(file_name)];
  ceph_assert(cc);

  bc = cc->db_cache_map[std::string(file_name)];
  vfs_context_map_mutex.unlock();

  return bc;
}

static
void set_lock_type(const std::string &db_name, const std::string &file_name, int eLock)
{
  vfs_context_map_mutex.lock();
  CephVFSContext *cc = vfs_context_map[db_name];
  ceph_assert(cc);

  cc->lock_info[file_name] = eLock;
  vfs_context_map_mutex.unlock();
}

static
int lock_file_in_rados(librados::IoCtx *io_ctx, const std::string &lock_file_name, const std::string &lock_name, bool exclusive)
{
  int ret = -1;
  int retries = 20;

  while (retries--) {
    if (exclusive)
      ret = io_ctx->lock_exclusive(lock_file_name, lock_name, get_cookie(), emptystr, NULL, 0);
    else
      ret = io_ctx->lock_shared(lock_file_name, lock_name, get_cookie(), emptystr, emptystr, NULL, 0);
    
    if (ret == 0)
      return ret;

    if (ret == -EBUSY)
      usleep(1000); // sleep for 1 milisecond
  }
  return ret;
}

static
int buffer_cache_write(const char *file_name, char *buf, int iAmt, sqlite_int64 iOfst, bool dirty, bool &merged_dirty_range)
{
  // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << "::" << file_name << ": length:" << std::dec << iAmt << ", offset:" << iOfst << std::endl;
  unsigned i = 0;
  libcephsqlite::BufferCache *bc = get_buffer_cache(file_name);
  unsigned page_size = bc->get_page_size();

  ceph_assert(bc);

  while (iAmt) {
    unsigned to_write = iAmt;

    if (to_write > page_size)
        to_write = page_size;

    /* even if to_write has been clamped to PAGE_SIZE, the actual write may
     * still span across 2 pages
     */
    unsigned pg_start = (iOfst + i) & ~(page_size - 1);
    unsigned pg_end   = (iOfst + i + to_write - 1) & ~(page_size - 1);
    if (pg_start != pg_end) {
      to_write = (pg_start + page_size) - (iOfst + i);
    }

    if (!bc->write(buf+i, iOfst+i, to_write, dirty, merged_dirty_range)) {
      // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << "::cache insert for " << file_name << " failed at (" << std::dec << to_write << ", " << (iOfst+i) << ")" << std::endl;
      return SQLITE_IOERR;
    } else {
      // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << "::inserted for " << file_name << " at (" << std::dec << to_write << ", " << (iOfst+i) << ")" << std::endl;
    }
    iAmt -= to_write;
    i += to_write;
  }
  return SQLITE_OK;
}

// for now its all-or-none
static
int buffer_cache_read(const char *file_name, char *buf, int iAmt, sqlite_int64 iOfst)
{
  unsigned i = 0;
  libcephsqlite::BufferCache *bc = get_buffer_cache(file_name);
  ceph_assert(bc);
  unsigned page_size = bc->get_page_size();


  while (iAmt) {
    unsigned to_lookup = iAmt;

    if (to_lookup > page_size)
        to_lookup = page_size;

    /* even if to_lookup has been clamped to PAGE_SIZE, the actual read may
     * still span across 2 pages
     */
    unsigned pg_start = (iOfst + i) & ~(page_size - 1);
    unsigned pg_end   = (iOfst + i + to_lookup - 1) & ~(page_size - 1);
    if (pg_start != pg_end) {
      to_lookup = (pg_start + page_size) - (iOfst + i);
    }

    if (!bc->read(buf+i, iOfst+i, to_lookup)) {
      // std::cerr << std::hex << pthread_self() << ":" << __FUNCTION__ << "::cache lookup for " << file_name << " failed at (" << std::dec << to_lookup << ", " << (iOfst+i) << ")" << std::endl;
      return SQLITE_IOERR_READ;
    }
    iAmt -= to_lookup;
    i += to_lookup;
  }
  return SQLITE_OK;
}

static
const char *get_rados_namespace(const char *uri_file_name)
{
  const char *p = uri_file_name;

  while (*p) {
    p += strlen(p); // point to the null char
    ++p; // skip the null char
    if (!*p) // check if there are any more 
      break;

    if (strncmp(p, "rados_namespace", 15) == 0) {
      p += strlen(p);
      ++p;
      ceph_assert(*p);
      return p;
    }
  }
  return NULL;
}

extern "C"
void ceph_sqlite3_dump_buffer_cache(sqlite3 *db, const char *file_name)
{
  libcephsqlite::BufferCache *bc = get_buffer_cache(file_name);
  if (bc)
    bc->dump();
}
