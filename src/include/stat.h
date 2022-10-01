#ifndef CEPH_STAT_H
#define CEPH_STAT_H

#include <acconfig.h>

#include <sys/stat.h>

/*
 * Access time-related `struct stat` members.
 *
 * Note that for each of the stat member get/set functions below, setting a
 * high-res value (stat_set_*_nsec) on a platform without high-res support is
 * a no-op.
 */

#ifdef HAVE_STAT_ST_MTIM_TV_NSEC

static inline uint32_t stat_get_mtime_nsec(struct stat *st)
{
  return st->st_mtim.tv_nsec;
}

static inline void stat_set_mtime_nsec(struct stat *st, uint32_t nsec)
{
  st->st_mtim.tv_nsec = nsec;
}

static inline uint32_t stat_get_atime_nsec(struct stat *st)
{
  return st->st_atim.tv_nsec;
}

static inline void stat_set_atime_nsec(struct stat *st, uint32_t nsec)
{
  st->st_atim.tv_nsec = nsec;
}

static inline uint32_t stat_get_ctime_nsec(struct stat *st)
{
  return st->st_ctim.tv_nsec;
}

static inline void stat_set_ctime_nsec(struct stat *st, uint32_t nsec)
{
  st->st_ctim.tv_nsec = nsec;
}

#elif defined(HAVE_STAT_ST_MTIMESPEC_TV_NSEC)

static inline uint32_t stat_get_mtime_nsec(struct stat *st)
{
  return st->st_mtimespec.tv_nsec;
}

static inline void stat_set_mtime_nsec(struct stat *st, uint32_t nsec)
{
  st->st_mtimespec.tv_nsec = nsec;
}

static inline uint32_t stat_get_atime_nsec(struct stat *st)
{
  return st->st_atimespec.tv_nsec;
}

static inline void stat_set_atime_nsec(struct stat *st, uint32_t nsec)
{
  st->st_atimespec.tv_nsec = nsec;
}

static inline uint32_t stat_get_ctime_nsec(struct stat *st)
{
  return st->st_ctimespec.tv_nsec;
}

static inline void stat_set_ctime_nsec(struct stat *st, uint32_t nsec)
{
  st->st_ctimespec.tv_nsec = nsec;
}

#else

static inline uint32_t stat_get_mtime_nsec(struct stat *st)
{
  return 0;
}

static inline void stat_set_mtime_nsec(struct stat *st, uint32_t nsec)
{
}

static inline uint32_t stat_get_atime_nsec(struct stat *st)
{
  return 0;
}

static inline void stat_set_atime_nsec(struct stat *st, uint32_t nsec)
{
}

static inline uint32_t stat_get_ctime_nsec(struct stat *st)
{
  return 0;
}

static inline void stat_set_ctime_nsec(struct stat *st, uint32_t nsec)
{
}

#endif

/*
 * Access second-resolution `struct stat` members.
 */

static inline uint32_t stat_get_mtime_sec(struct stat *st)
{
  return st->st_mtime;
}

static inline void stat_set_mtime_sec(struct stat *st, uint32_t sec)
{
  st->st_mtime = sec;
}

static inline uint32_t stat_get_atime_sec(struct stat *st)
{
  return st->st_atime;
}

static inline void stat_set_atime_sec(struct stat *st, uint32_t sec)
{
  st->st_atime = sec;
}

static inline uint32_t stat_get_ctime_sec(struct stat *st)
{
  return st->st_ctime;
}

static inline void stat_set_ctime_sec(struct stat *st, uint32_t sec)
{
  st->st_ctime = sec;
}

#endif
