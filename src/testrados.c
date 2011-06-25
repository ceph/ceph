// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
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
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

/* Print out a line, but prefix the thread number. */
static void tprintf(int tnum, const char *fmt, ...)
{
	va_list ap;
	printf("%d :", tnum);
	va_start(ap, fmt);
	vprintf(fmt, ap);
	va_end(ap);
}

static int do_rados_setxattr(int tnum, rados_ioctx_t io_ctx, const char *oid,
			const char *key, const char *val)
{
	int ret = rados_setxattr(io_ctx, oid, key, val, strlen(val) + 1);
	if (ret < 0) {
		tprintf(tnum, "rados_setxattr failed with error %d\n", ret);
		return 1;
	}
	tprintf(tnum, "rados_setxattr %s=%s\n", key, val);
	return 0;
}

static int do_rados_getxattr(int tnum, rados_ioctx_t io_ctx, const char *oid,
			const char *key, const char *expected)
{
	size_t blen = strlen(expected) + 1;
	char buf[blen];
	memset(buf, 0, sizeof(buf));
	int r = rados_getxattr(io_ctx, oid, key, buf, blen);
	if (r < 0) {
		tprintf(tnum, "rados_getxattr(%s) failed with error %d\n", key, r);
		return 1;
	}
	if (strcmp(buf, expected) != 0) {
		tprintf(tnum, "rados_getxattr(%s) got wrong result! "
		       "expected: '%s'. got '%s'\n", key, expected, buf);
		return 1;
	}
	tprintf(tnum, "rados_getxattr %s=%s\n", key, buf);
	return 0;
}

static int do_rados_getxattrs(int tnum, rados_ioctx_t io_ctx, const char *oid,
			const char **exkeys, const char **exvals)
{
	rados_xattrs_iter_t iter;
	int nval = 0, i, nfound = 0, ret = 0;

	for (i = 0; exvals[i]; ++i) {
		++nval;
	}
	ret = rados_getxattrs(io_ctx, oid, &iter);
	if (ret) {
		tprintf(tnum, "rados_getxattrs(%s) failed with error %d\n", oid, ret);
		return 1;
	}
	while (1) {
	        size_t len;
	        const char *key, *val;
		ret = rados_getxattrs_next(iter, &key, &val, &len);
		if (ret) {
			tprintf(tnum, "rados_getxattrs(%s): rados_getxattrs_next "
				"returned error %d\n", oid, ret);
			return 1;
		}
		if (!key)
			break;
		for (i = 0; i < nval; ++i) {
			if (strcmp(exkeys[i], key))
				continue;
			if ((len == strlen(exvals[i]) + 1) && (!strcmp(exvals[i], val))) {
				nfound++;
				break;
			}
			tprintf(tnum, "rados_getxattrs(%s): got key %s, but the "
				"value was %s rather than %s.\n",
				oid, key, val, exvals[i]);
			return 1;
		}
	}
	if (nfound != nval) {
		tprintf(tnum, "rados_getxattrs(%s): only found %d extended attributes. "
			"Expected %d\n", oid, nfound, nval);
		return 1;
	}
	rados_getxattrs_end(iter);
	tprintf(tnum, "rados_getxattrs(%s)\n", oid);
	return 0;
}

static int testrados(int tnum)
{
	char tmp[32];
	int i, r;
	rados_t cl;

	if (rados_create(&cl, NULL) < 0) {
		tprintf(tnum, "error initializing\n");
		return 1;
	}

	if (rados_conf_read_file(cl, NULL)) {
		tprintf(tnum, "error reading configuration file\n");
		return 1;
	}

	// Try to set a configuration option that doesn't exist.
	// This should fail.
	if (!rados_conf_set(cl, "config option that doesn't exist",
			"some random value")) {
		tprintf(tnum, "error: succeeded in setting nonexistent config option\n");
		return 1;
	}

	if (rados_conf_get(cl, "log to stderr", tmp, sizeof(tmp))) {
		tprintf(tnum, "error: failed to read log_to_stderr from config\n");
		return 1;
	}

	// Can we change it?
	if (rados_conf_set(cl, "log to stderr", "2")) {
		tprintf(tnum, "error: error setting log_to_stderr\n");
		return 1;
	}
	if (rados_conf_get(cl, "log to stderr", tmp, sizeof(tmp))) {
		tprintf(tnum, "error: failed to read log_to_stderr from config\n");
		return 1;
	}
	if (tmp[0] != '2') {
		tprintf(tnum, "error: new setting for log_to_stderr failed to take effect.\n");
		return 1;
	}

	if (rados_connect(cl)) {
		tprintf(tnum, "error connecting\n");
		return 1;
	}
	if (rados_connect(cl) == 0) {
		tprintf(tnum, "second connect attempt didn't return an error\n");
		return 1;
	}

	/* create an io_ctx */
	r = rados_pool_create(cl, "foo");
	tprintf(tnum, "rados_pool_create = %d\n", r);

	rados_ioctx_t io_ctx;
	r = rados_ioctx_create(cl, "foo", &io_ctx);
	tprintf(tnum, "rados_ioctx_create = %d, io_ctx = %p\n", r, io_ctx);

	/* list all pools */
	{
		int buf_sz = rados_pool_list(cl, NULL, 0);
		tprintf(tnum, "need buffer size of %d\n", buf_sz);
		char buf[buf_sz];
		int r = rados_pool_list(cl, buf, buf_sz);
		if (r != buf_sz) {
			tprintf(tnum, "buffer size mismatch: got %d the first time, but %d "
			"the second.\n", buf_sz, r);
			return 1;
		}
		const char *b = buf;
		tprintf(tnum, "begin pools.\n");
		while (1) {
		if (b[0] == '\0')
		break;
		tprintf(tnum, " pool: '%s'\n", b);
		b += strlen(b) + 1;
		};
		tprintf(tnum, "end pools.\n");
	}


	/* stat */
	struct rados_pool_stat_t st;
	r = rados_ioctx_pool_stat(io_ctx, &st);
	tprintf(tnum, "rados_ioctx_pool_stat = %d, %lld KB, %lld objects\n", r, (long long)st.num_kb, (long long)st.num_objects);

	/* snapshots */
	r = rados_ioctx_snap_create(io_ctx, "snap1");
	tprintf(tnum, "rados_ioctx_snap_create snap1 = %d\n", r);
	rados_snap_t snaps[10];
	r = rados_ioctx_snap_list(io_ctx, snaps, 10);
	for (i=0; i<r; i++) {
		char name[100];
		rados_ioctx_snap_get_name(io_ctx, snaps[i], name, sizeof(name));
		tprintf(tnum, "rados_ioctx_snap_list got snap %lld %s\n", (long long)snaps[i], name);
	}
	rados_snap_t snapid;
	r = rados_ioctx_snap_lookup(io_ctx, "snap1", &snapid);
	tprintf(tnum, "rados_ioctx_snap_lookup snap1 got %lld, result %d\n", (long long)snapid, r);
	r = rados_ioctx_snap_remove(io_ctx, "snap1");
	tprintf(tnum, "rados_ioctx_snap_remove snap1 = %d\n", r);

	/* sync io */
	time_t tm;
	char buf[128], buf2[128];
	time(&tm);
	snprintf(buf, 128, "%s", ctime(&tm));
	const char *oid = "foo_object";
	r = rados_write(io_ctx, oid, buf, strlen(buf) + 1, 0);
	tprintf(tnum, "rados_write = %d\n", r);
	r = rados_read(io_ctx, oid, buf2, sizeof(buf2), 0);
	tprintf(tnum, "rados_read = %d\n", r);
	if (memcmp(buf, buf2, r))
		tprintf(tnum, "*** content mismatch ***\n");

	/* attrs */
	if (do_rados_setxattr(tnum, io_ctx, oid, "b", "2"))
		return 1;
	if (do_rados_setxattr(tnum, io_ctx, oid, "a", "1"))
		return 1;
	if (do_rados_setxattr(tnum, io_ctx, oid, "c", "3"))
		return 1;
	if (do_rados_getxattr(tnum, io_ctx, oid, "a", "1"))
		return 1;
	if (do_rados_getxattr(tnum, io_ctx, oid, "b", "2"))
		return 1;
	if (do_rados_getxattr(tnum, io_ctx, oid, "c", "3"))
		return 1;
	const char *exkeys[] = { "a", "b", "c", NULL };
	const char *exvals[] = { "1", "2", "3", NULL };
	if (do_rados_getxattrs(tnum, io_ctx, oid, exkeys, exvals))
		return 1;

	uint64_t size;
	time_t mtime;
	r = rados_stat(io_ctx, oid, &size, &mtime);
	tprintf(tnum, "rados_stat size = %lld mtime = %d = %d\n", (long long)size, (int)mtime, r);
	r = rados_stat(io_ctx, "does_not_exist", NULL, NULL);
	tprintf(tnum, "rados_stat(does_not_exist) = %d\n", r);

	/* exec */
	rados_exec(io_ctx, oid, "crypto", "md5", buf, strlen(buf) + 1, buf, 128);
	tprintf(tnum, "exec result=%s\n", buf);
	r = rados_read(io_ctx, oid, buf2, 128, 0);
	tprintf(tnum, "read result=%s\n", buf2);
	tprintf(tnum, "size=%d\n", r);

	/* aio */
	rados_completion_t a, b;
	rados_aio_create_completion(0, 0, 0, &a);
	rados_aio_create_completion(0, 0, 0, &b);
	rados_aio_write(io_ctx, "a", a, buf, 100, 0);
	rados_aio_write(io_ctx, "../b/bb_bb_bb\\foo\\bar", b, buf, 100, 0);
	rados_aio_wait_for_safe(a);
	tprintf(tnum, "a safe\n");
	rados_aio_wait_for_safe(b);
	tprintf(tnum, "b safe\n");
	rados_aio_release(a);
	rados_aio_release(b);

	/* test flush */
	tprintf(tnum, "testing aio flush\n");
	rados_completion_t c;
	rados_aio_create_completion(0, 0, 0, &c);
	rados_aio_write(io_ctx, "c", c, buf, 100, 0);
	int safe = rados_aio_is_safe(c);
	tprintf(tnum, "a should not yet be safe and ... %s\n", safe ? "is":"is not");
	assert(!safe);
	rados_aio_flush(io_ctx);
	safe = rados_aio_is_safe(c);
	tprintf(tnum, "a should be safe and ... %s\n", safe ? "is":"is not");
	assert(safe);
	rados_aio_release(c);
	
	rados_read(io_ctx, "../b/bb_bb_bb\\foo\\bar", buf2, 128, 0);

	/* list objects */
	rados_list_ctx_t h;
	r = rados_objects_list_open(io_ctx, &h);
	tprintf(tnum, "rados_list_objects_open = %d, h = %p\n", r, h);
	const char *poolname;
	while (rados_objects_list_next(h, &poolname) == 0)
		tprintf(tnum, "rados_list_objects_next got object '%s'\n", poolname);
	rados_objects_list_close(h);

	/* stat */
	r = rados_ioctx_pool_stat(io_ctx, &st);
	tprintf(tnum, "rados_stat_pool = %d, %lld KB, %lld objects\n", r, (long long)st.num_kb, (long long)st.num_objects);

	/* delete a pool */
	tprintf(tnum, "rados_delete_pool = %d\n", r);
	rados_ioctx_destroy(io_ctx);

	r = rados_pool_delete(cl, "foo");
	tprintf(tnum, "rados_ioctx_pool_delete = %d\n", r);

	rados_shutdown(cl);
	return 0;
}

void *do_testrados(void *v)
{
	int thread_number = (int)(intptr_t)v;
	return testrados(thread_number) ? (void*)0x1 : NULL;
}

int main(int argc, const char **argv)
{
	int ret = 0, i, num_threads = 1;
	char *num_threads_option = getenv("NUM_THREADS");
	if (num_threads_option)
		num_threads = atoi(num_threads_option);
	if (num_threads < 1) {
		printf("can't have num_threads < 1\n");
		return EXIT_FAILURE;
	}
	pthread_t threads[num_threads];
	for (i = 0; i < num_threads; ++i) {
		if (pthread_create(&threads[i], NULL, do_testrados,
				   (void*)(intptr_t)i)) {
			printf("pthread_create failed.\n");
			return EXIT_FAILURE;
		}
	}
	for (i = 0; i < num_threads; ++i) {
		void* tret;
		if (pthread_join(threads[i], &tret)) {
			printf("pthread_join failed.\n");
			return EXIT_FAILURE;
		}
		if (tret != NULL) {
			printf("thread %d failed!\n", i);
			ret = 1;
		}
	}
	if (ret)
		printf("FAILURE\n");
	else
		printf("SUCCESS\n");
	return ret;
}
