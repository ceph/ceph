#ifndef TEST_SEMAPHORE_COMPAT_H
#define TEST_SEMAPHORE_COMPAT_H

#include <semaphore.h>

#ifdef __APPLE__

#include <time.h>
#include "include/ceph_assert.h"
#include "unistd.h"
// Apple's implementation of the posix semaphores
// lacks the timedwait. For the purpose of the test
// it is sufficient to emulate that with a sleep poll loop

static int64_t us_until(const struct timespec *when)
{
    const long nsec_in_sec = 1000000000;
    const long usec_in_sec = 1000000;
    const long nsec_in_usec = nsec_in_sec / usec_in_sec;
    struct timespec now;

    if (clock_gettime(CLOCK_REALTIME, &now) == -1) {
        ceph_abort();
    }

    int64_t usec_diff = (when->tv_nsec - now.tv_nsec) / nsec_in_usec;
    usec_diff += (when->tv_sec - now.tv_sec) * usec_in_sec;
    return usec_diff;
}

static int sem_timedwait(sem_t *sem, const struct timespec *ts)
{
    int64_t poll_interval_us = -1;
  
    do {
        if (0 == sem_trywait(sem)) {
            // success!
            return 0;
        }
        if (EAGAIN != errno) {
            break;
        }
        // need to wait until ...
        int64_t us_to_wait = us_until(ts);
        if (us_to_wait <= 0) {
            errno = ETIMEDOUT;
            break;
        }
        if (poll_interval_us < 0) {
            // pick a reasonable poll interval
            poll_interval_us = us_to_wait / 10;
        }
        usleep((useconds_t)std::min(us_to_wait, poll_interval_us));
    } while (true);
    return -1;
}

#endif
#endif // TEST_SEMAPHORE_COMPAT_H
