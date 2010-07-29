#ifndef CEPH_ERR_H
#define CEPH_ERR_H

/*
 * adapted from linux 2.6.24 include/linux/err.h
 */
#define MAX_ERRNO 4095
#define IS_ERR_VALUE(x) ((x) >= (unsigned long)-MAX_ERRNO)

#include <errno.h>

/* this generates a warning in c++; caller can do the cast manually
static inline void *ERR_PTR(long error)
{
  return (void *) error;
}
*/

static inline long PTR_ERR(const void *ptr)
{
  return (long) ptr;
}

static inline long IS_ERR(const void *ptr)
{
  return IS_ERR_VALUE((unsigned long)ptr);
}

#endif
