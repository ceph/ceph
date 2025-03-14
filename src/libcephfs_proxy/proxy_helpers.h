
#ifndef __LIBCEPHFS_PROXY_HELPERS_H__
#define __LIBCEPHFS_PROXY_HELPERS_H__

#include <stdlib.h>
#include <signal.h>
#include <pthread.h>
#include <stdint.h>
#include <stdbool.h>

#include "proxy_log.h"

#define __public __attribute__((__visibility__("default")))

#define ptr_value(_ptr) ((uint64_t)(uintptr_t)(_ptr))
#define value_ptr(_val) ((void *)(uintptr_t)(_val))

typedef struct _proxy_random {
	uint64_t mask;
	uint64_t factor;
	uint64_t factor_inv;
	uint64_t shift;
} proxy_random_t;

/* Generate a 64-bits random number different than 0. */
static inline uint64_t random_u64(void)
{
	uint64_t value;
	int32_t i;

	do {
		value = 0;
		for (i = 0; i < 4; i++) {
			value <<= 16;
			value ^= (random() >> 8) & 0xffff;
		}
	} while (value == 0);

	return value;
}

/* Randomly initialize the data used to scramble pointers. */
static inline void random_init(proxy_random_t *rnd)
{
	uint64_t inv;

	rnd->mask = random_u64();

	/* Generate an odd multiplicative factor different than 1. */
	do {
		rnd->factor = random_u64() | 1;
	} while (rnd->factor == 1);

	/* Compute the inverse of 'factor' modulo 2^64. */
	inv = rnd->factor & 0x3;
	inv *= 0x000000012 - rnd->factor * inv;
	inv *= 0x000000102 - rnd->factor * inv;
	inv *= 0x000010002 - rnd->factor * inv;
	inv *= 0x100000002 - rnd->factor * inv;
	rnd->factor_inv = inv * (2 - rnd->factor * inv);

	rnd->shift = random_u64();
}

/* Obfuscate a pointer. */
static inline uint64_t random_scramble(proxy_random_t *rnd, uint64_t value)
{
	uint32_t bits;

	bits = __builtin_popcountll(value);

	/* rnd->shift is rotated by the amount of bits set to 1 in the original
	 * value, and the lowest 6 bits are extracted. This generates a
	 * pseudo-random number that depends on the number of bits of the
	 * value. */
	bits = ((rnd->shift >> bits) | (rnd->shift << (64 - bits))) & 0x3f;

	/* The value is rotated by the amount just computed. */
	value = (value << bits) | (value >> (64 - bits));

	/* The final result is masked with a random number. */
	value ^= rnd->mask;

	/* And multiplied by a random factor modulo 2^64. */
	return value * rnd->factor;
}

/* Recover a pointer. */
static inline uint64_t random_unscramble(proxy_random_t *rnd, uint64_t value)
{
	uint32_t bits;

	/* Divide by the random factor (i.e. multiply by the inverse of the
	 * factor). */
	value *= rnd->factor_inv;

	/* Remove the mask. */
	value ^= rnd->mask;

	/* Get the number of bits the pointer was rotated. */
	bits = __builtin_popcountll(value);
	bits = ((rnd->shift >> bits) | (rnd->shift << (64 - bits))) & 0x3f;

	/* Undo the rotation to recover the original value. */
	return (value >> bits) | (value << (64 - bits));
}

static inline uint64_t uint64_checksum(uint64_t value)
{
	value = (value & 0xff00ff00ff00ffULL) +
		((value >> 8) & 0xff00ff00ff00ffULL);
	value += value >> 16;
	value += value >> 32;

	return value & 0xff;
}

static inline uint64_t ptr_checksum(proxy_random_t *rnd, void *ptr)
{
	uint64_t value;

	if (ptr == NULL) {
		return 0;
	}

	value = (uint64_t)(uintptr_t)ptr;
	/* Many current processors don't use the full 64-bits for the virtual
         * address space, and Linux assigns the lower 128 TiB (47 bits) for
         * user-space applications on most architectures, so the highest 8 bits
         * of all valid addressess are always 0.
         *
         * We use this to encode a checksum in the high byte of the address to
         * be able to do a verification before dereferencing the pointer,
	 * avoiding crashes if the client passes an invalid or corrupted pointer
	 * value.
         *
         * Alternatives like using indexes in a table or registering valid
	 * pointers require access to a shared data structure that will require
	 * thread synchronization, making it slower. */
	if ((value & 0xff00000000000007ULL) != 0) {
		proxy_log(LOG_ERR, EINVAL,
			  "Unexpected pointer value");
		abort();
	}

	value -= uint64_checksum(value) << 56;

	return random_scramble(rnd, value);
}

static inline int32_t ptr_check(proxy_random_t *rnd, uint64_t value,
				void **pptr)
{
	if (value == 0) {
		*pptr = NULL;
		return 0;
	}

	value = random_unscramble(rnd, value);

	if ((uint64_checksum(value) != 0) || ((value & 7) != 0)) {
		proxy_log(LOG_ERR, EFAULT, "Unexpected pointer value");
		return -EFAULT;
	}

	*pptr = (void *)(uintptr_t)(value & 0xffffffffffffffULL);

	return 0;
}

static inline void *proxy_malloc(size_t size)
{
	void *ptr;

	ptr = malloc(size);
	if (ptr == NULL) {
		proxy_log(LOG_ERR, errno, "Failed to allocate memory");
	}

	return ptr;
}

static inline int32_t proxy_realloc(void **pptr, size_t size)
{
	void *ptr;

	ptr = realloc(*pptr, size);
	if (ptr == NULL) {
		return proxy_log(LOG_ERR, errno, "Failed to reallocate memory");
	}

	*pptr = ptr;

	return 0;
}

static inline void proxy_free(void *ptr)
{
	free(ptr);
}

static inline char *proxy_strdup(const char *str)
{
	char *ptr;

	ptr = strdup(str);
	if (ptr == NULL) {
		proxy_log(LOG_ERR, errno, "Failed to copy a string");
		return NULL;
	}

	return ptr;
}

static inline int32_t proxy_mutex_init(pthread_mutex_t *mutex)
{
	int32_t err;

	err = pthread_mutex_init(mutex, NULL);
	if (err != 0) {
		return proxy_log(LOG_ERR, err, "Failed to initialize a mutex");
	}

	return 0;
}

static inline void proxy_mutex_lock(pthread_mutex_t *mutex)
{
	int32_t err;

	err = pthread_mutex_lock(mutex);
	if (err != 0) {
		proxy_abort(err, "Mutex cannot be acquired");
	}
}

static inline void proxy_mutex_unlock(pthread_mutex_t *mutex)
{
	int32_t err;

	err = pthread_mutex_unlock(mutex);
	if (err != 0) {
		proxy_abort(err, "Mutex cannot be released");
	}
}

static inline int32_t proxy_rwmutex_init(pthread_rwlock_t *mutex)
{
	int32_t err;

	err = pthread_rwlock_init(mutex, NULL);
	if (err != 0) {
		return proxy_log(LOG_ERR, err,
				 "Failed to initialize a rwmutex");
	}

	return 0;
}

static inline void proxy_rwmutex_rdlock(pthread_rwlock_t *mutex)
{
	int32_t err;

	err = pthread_rwlock_rdlock(mutex);
	if (err != 0) {
		proxy_abort(err, "RWMutex cannot be acquired for read");
	}
}

static inline void proxy_rwmutex_wrlock(pthread_rwlock_t *mutex)
{
	int32_t err;

	err = pthread_rwlock_wrlock(mutex);
	if (err != 0) {
		proxy_abort(err, "RWMutex cannot be acquired for write");
	}
}

static inline void proxy_rwmutex_unlock(pthread_rwlock_t *mutex)
{
	int32_t err;

	err = pthread_rwlock_unlock(mutex);
	if (err != 0) {
		proxy_abort(err, "RWMutex cannot be released");
	}
}

static inline int32_t proxy_condition_init(pthread_cond_t *condition)
{
	int32_t err;

	err = pthread_cond_init(condition, NULL);
	if (err != 0) {
		return proxy_log(LOG_ERR, err,
				 "Failed to initialize a condition variable");
	}

	return 0;
}

static inline void proxy_condition_signal(pthread_cond_t *condition)
{
	int32_t err;

	err = pthread_cond_signal(condition);
	if (err != 0) {
		proxy_abort(err, "Condition variable cannot be signaled");
	}
}

static inline void proxy_condition_wait(pthread_cond_t *condition,
					pthread_mutex_t *mutex)
{
	int32_t err;

	err = pthread_cond_wait(condition, mutex);
	if (err != 0) {
		proxy_abort(err, "Condition variable cannot be waited");
	}
}

static inline int32_t proxy_thread_create(pthread_t *tid,
					  void *(*start)(void *), void *arg)
{
	int32_t err;

	err = pthread_create(tid, NULL, start, arg);
	if (err != 0) {
		proxy_log(LOG_ERR, err, "Failed to create a thread");
	}

	return err;
}

static inline void proxy_thread_kill(pthread_t tid, int32_t signum)
{
	int32_t err;

	err = pthread_kill(tid, signum);
	if (err != 0) {
		proxy_abort(err, "Failed to send a signal to a thread");
	}
}

static inline void proxy_thread_join(pthread_t tid)
{
	int32_t err;

	err = pthread_join(tid, NULL);
	if (err != 0) {
		proxy_log(LOG_ERR, err, "Unable to join a thread");
	}
}

static inline int32_t proxy_signal_set(int32_t signum, struct sigaction *action,
				       struct sigaction *old)
{
	if (sigaction(signum, action, old) < 0) {
		return proxy_log(LOG_ERR, errno,
				 "Failed to configure a signal");
	}

	return 0;
}

int32_t proxy_hash(uint8_t *hash, size_t size,
		   int32_t (*feed)(void **, void *, int32_t), void *data);

int32_t proxy_hash_hex(char *digest, size_t size,
		       int32_t (*feed)(void **, void *, int32_t), void *data);

#endif
