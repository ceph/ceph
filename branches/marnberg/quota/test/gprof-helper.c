/* gprof-helper.c -- preload library to profile pthread-enabled programs
 *
 * Authors: Sam Hocevar <sam at zoy dot org>
 *          Daniel Jönsson <danieljo at fagotten dot org>
 *
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the Do What The Fuck You Want To
 *  Public License as published by Banlu Kemiyatorn. See
 *  http://sam.zoy.org/projects/COPYING.WTFPL for more details.
 *
 * Compilation example:
 * gcc -shared -fPIC gprof-helper.c -o gprof-helper.so -lpthread -ldl
 *
 * Usage example:
 * LD_PRELOAD=./gprof-helper.so your_program
 */

#define _GNU_SOURCE
#include <sys/time.h>
#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>
#include <pthread.h>

static void * wrapper_routine(void *);

/* Original pthread function */
static int (*pthread_create_orig)(pthread_t *__restrict,
                                  __const pthread_attr_t *__restrict,
                                  void *(*)(void *),
                                  void *__restrict) = NULL;

/* Library initialization function */
void wooinit(void) __attribute__((constructor));

void wooinit(void)
{
    pthread_create_orig = dlsym(RTLD_NEXT, "pthread_create");
    fprintf(stderr, "pthreads: using profiling hooks for gprof\n");
    if(pthread_create_orig == NULL)
    {
        char *error = dlerror();
        if(error == NULL)
        {
            error = "pthread_create is NULL";
        }
        fprintf(stderr, "%s\n", error);
        exit(EXIT_FAILURE);
    }
}

/* Our data structure passed to the wrapper */
typedef struct wrapper_s
{
    void * (*start_routine)(void *);
    void * arg;

    pthread_mutex_t lock;
    pthread_cond_t  wait;

    struct itimerval itimer;

} wrapper_t;

/* The wrapper function in charge for setting the itimer value */
static void * wrapper_routine(void * data)
{
    /* Put user data in thread-local variables */
    void * (*start_routine)(void *) = ((wrapper_t*)data)->start_routine;
    void * arg = ((wrapper_t*)data)->arg;

    /* Set the profile timer value */
    setitimer(ITIMER_PROF, &((wrapper_t*)data)->itimer, NULL);

    /* Tell the calling thread that we don't need its data anymore */
    pthread_mutex_lock(&((wrapper_t*)data)->lock);
    pthread_cond_signal(&((wrapper_t*)data)->wait);
    pthread_mutex_unlock(&((wrapper_t*)data)->lock);

    /* Call the real function */
    return start_routine(arg);
}

/* Our wrapper function for the real pthread_create() */
int pthread_create(pthread_t *__restrict thread,
                   __const pthread_attr_t *__restrict attr,
                   void * (*start_routine)(void *),
                   void *__restrict arg)
{
    wrapper_t wrapper_data;
    int i_return;

    /* Initialize the wrapper structure */
    wrapper_data.start_routine = start_routine;
    wrapper_data.arg = arg;
    getitimer(ITIMER_PROF, &wrapper_data.itimer);
    pthread_cond_init(&wrapper_data.wait, NULL);
    pthread_mutex_init(&wrapper_data.lock, NULL);
    pthread_mutex_lock(&wrapper_data.lock);

    /* The real pthread_create call */
    i_return = pthread_create_orig(thread,
                                   attr,
                                   &wrapper_routine,
                                   &wrapper_data);

    /* If the thread was successfully spawned, wait for the data
     * to be released */
    if(i_return == 0)
    {
        pthread_cond_wait(&wrapper_data.wait, &wrapper_data.lock);
    }

    pthread_mutex_unlock(&wrapper_data.lock);
    pthread_mutex_destroy(&wrapper_data.lock);
    pthread_cond_destroy(&wrapper_data.wait);

    return i_return;
}

