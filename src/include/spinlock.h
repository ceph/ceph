#ifndef CEPH_SPINLOCK_H
#define CEPH_SPINLOCK_H

#include <pthread.h>

#include "acconfig.h"

typedef struct {
#ifdef HAVE_PTHREAD_SPINLOCK
  pthread_spinlock_t lock;
#else
  pthread_mutex_t lock;
#endif
} ceph_spinlock_t;

#ifdef HAVE_PTHREAD_SPINLOCK

static inline int ceph_spin_init(ceph_spinlock_t *l)
{
  return pthread_spin_init(&l->lock, PTHREAD_PROCESS_PRIVATE);
}

static inline int ceph_spin_destroy(ceph_spinlock_t *l)
{
  return pthread_spin_destroy(&l->lock);
}

static inline int ceph_spin_lock(ceph_spinlock_t *l)
{
  return pthread_spin_lock(&l->lock);
}

static inline int ceph_spin_unlock(ceph_spinlock_t *l)
{
  return pthread_spin_unlock(&l->lock);
}

#else /* !HAVE_PTHREAD_SPINLOCK */

static inline int ceph_spin_init(ceph_spinlock_t *l)
{
  return pthread_mutex_init(&l->lock, NULL);
}

static inline int ceph_spin_destroy(ceph_spinlock_t *l)
{
  return pthread_mutex_destroy(&l->lock);
}

static inline int ceph_spin_lock(ceph_spinlock_t *l)
{
  return pthread_mutex_lock(&l->lock);
}

static inline int ceph_spin_unlock(ceph_spinlock_t *l)
{
  return pthread_mutex_unlock(&l->lock);
}

#endif

#endif
