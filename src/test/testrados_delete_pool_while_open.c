// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/rados/librados.h"

#include <assert.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>

static sem_t creation_sem;
static sem_t destruction_sem;

/* sucess / failure codes for thread1 and thread2 */
#define SUCCESS ((void*)NULL)
#define FAILURE ((void*)0x1)

static void* do_thread1(void *v)
{
	rados_t cl;
	if (rados_create(&cl, NULL) < 0) {
		printf("error initializing\n");
		return FAILURE;
	}
	if (rados_conf_read_file(cl, NULL)) {
		printf("error reading configuration file\n");
		return FAILURE;
	}
	if (rados_connect(cl)) {
		printf("error connecting\n");
		return FAILURE;
	}
	if (rados_pool_create(cl, "foo")) {
		printf("rados_pool_create error\n");
		return FAILURE;
	}
	rados_ioctx_t io_ctx;
	if (rados_ioctx_create(cl, "foo", &io_ctx)) {
		printf("rados_ioctx_create error\n");
		return FAILURE;
	}
	sem_post(&creation_sem);
	sem_wait(&destruction_sem);
	struct rados_pool_stat_t st;
	int ret = rados_ioctx_pool_stat(io_ctx, &st);
	if (ret) {
		printf("rados_ioctx_pool_stat failed with error %d\n", ret);
	}
	else {
		/* This should not succeed because the pool was deleted
		 * by the other thread. */
		printf("rados_ioctx_pool_stat succeeded\n", ret);
		return FAILURE;
	}
	rados_ioctx_destroy(io_ctx);
	return SUCCESS;
}

static void* do_thread2(void *v)
{
	rados_t cl;
	if (rados_create(&cl, NULL) < 0) {
		printf("error initializing\n");
		return FAILURE;
	}
	if (rados_conf_read_file(cl, NULL)) {
		printf("error reading configuration file\n");
		return FAILURE;
	}
	if (rados_connect(cl)) {
		printf("error connecting\n");
		return FAILURE;
	}
	sem_wait(&creation_sem);
	if (rados_pool_delete(cl, "foo")) {
		printf("rados_pool_delete error\n");
		return FAILURE;
	}
	sem_post(&destruction_sem);
	return SUCCESS;
}

int main(void)
{
	void *r;
	int ret = 0;
	pthread_t thread1, thread2;

	if (sem_init(&creation_sem, 0, 0)) {
		printf("sem_init failed.\n");
		return EXIT_FAILURE;
	}
	if (sem_init(&destruction_sem, 0, 0)) {
		printf("sem_init failed.\n");
		return EXIT_FAILURE;
	}

	/* create threads */
	if (pthread_create(&thread1, NULL, do_thread1, NULL)) {
		printf("pthread_create failed.\n");
		return EXIT_FAILURE;
	}
	if (pthread_create(&thread2, NULL, do_thread2, NULL)) {
		printf("pthread_create failed.\n");
		return EXIT_FAILURE;
	}

	/* wait for threads */
	if (pthread_join(thread1, &r)) {
		printf("pthread_join failed.\n");
		return EXIT_FAILURE;
	}
	if (r != SUCCESS)
		ret = 1;
	if (pthread_join(thread2, &r)) {
		printf("pthread_join failed.\n");
		return EXIT_FAILURE;
	}
	if (r != SUCCESS)
		ret = 1;

	/* done */
	if (ret)
		printf("FAILURE\n");
	else
		printf("SUCCESS\n");
	return ret;
}
