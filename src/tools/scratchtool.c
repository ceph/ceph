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
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

static int do_rados_setxattr(rados_ioctx_t io_ctx, const char *oid,
			const char *key, const char *val)
{
	int ret = rados_setxattr(io_ctx, oid, key, val, strlen(val) + 1);
	if (ret < 0) {
		printf("rados_setxattr failed with error %d\n", ret);
		return 1;
	}
	printf("rados_setxattr %s=%s\n", key, val);
	return 0;
}

static int do_rados_getxattr(rados_ioctx_t io_ctx, const char *oid,
			const char *key, const char *expected)
{
	size_t blen = strlen(expected) + 1;
	char buf[blen];
	memset(buf, 0, sizeof(buf));
	int r = rados_getxattr(io_ctx, oid, key, buf, blen);
	if (r < 0) {
		printf("rados_getxattr(%s) failed with error %d\n", key, r);
		return 1;
	}
	if (strcmp(buf, expected) != 0) {
		printf("rados_getxattr(%s) got wrong result! "
		       "expected: '%s'. got '%s'\n", key, expected, buf);
		return 1;
	}
	printf("rados_getxattr %s=%s\n", key, buf);
	return 0;
}

static int do_rados_getxattrs(rados_ioctx_t io_ctx, const char *oid,
			const char **exkeys, const char **exvals)
{
	rados_xattrs_iter_t iter;
	int nval = 0, i, nfound = 0, r = 0, ret = 1;

	for (i = 0; exvals[i]; ++i) {
		++nval;
	}
	r = rados_getxattrs(io_ctx, oid, &iter);
	if (r) {
		printf("rados_getxattrs(%s) failed with error %d\n", oid, r);
		return 1;
	}
	while (1) {
	        size_t len;
	        const char *key, *val;
		r = rados_getxattrs_next(iter, &key, &val, &len);
		if (r) {
			printf("rados_getxattrs(%s): rados_getxattrs_next "
				"returned error %d\n", oid, r);
			goto out_err;
		}
		if (!key)
			break;
		for (i = 0; i < nval; ++i) {
			if (strcmp(exkeys[i], key))
				continue;
			if ((len == strlen(exvals[i]) + 1) && (val != NULL) && (!strcmp(exvals[i], val))) {
				nfound++;
				break;
			}
			printf("rados_getxattrs(%s): got key %s, but the "
				"value was %s rather than %s.\n",
				oid, key, val, exvals[i]);
			goto out_err;
		}
	}
	if (nfound != nval) {
		printf("rados_getxattrs(%s): only found %d extended attributes. "
			"Expected %d\n", oid, nfound, nval);
		goto out_err;
	}
	ret = 0;
	printf("rados_getxattrs(%s)\n", oid);

out_err:
	rados_getxattrs_end(iter);
	return ret;
}

static int testrados(void)
{
	char tmp[32];
	int i, r;
	int ret = 1; //set 1 as error case
	rados_t cl;

	if (rados_create(&cl, NULL) < 0) {
		printf("error initializing\n");
		return 1;
	}

	if (rados_conf_read_file(cl, NULL)) {
		printf("error reading configuration file\n");
		goto out_err;
	}

	// Try to set a configuration option that doesn't exist.
	// This should fail.
	if (!rados_conf_set(cl, "config option that doesn't exist",
			"some random value")) {
		printf("error: succeeded in setting nonexistent config option\n");
		goto out_err;
	}

	if (rados_conf_get(cl, "log to stderr", tmp, sizeof(tmp))) {
		printf("error: failed to read log_to_stderr from config\n");
		goto out_err;
	}

	// Can we change it?
	if (rados_conf_set(cl, "log to stderr", "true")) {
		printf("error: error setting log_to_stderr\n");
		goto out_err;
	}
	if (rados_conf_get(cl, "log to stderr", tmp, sizeof(tmp))) {
		printf("error: failed to read log_to_stderr from config\n");
		goto out_err;
	}
	if (strcmp(tmp, "true")) {
		printf("error: new setting for log_to_stderr failed to take effect.\n");
		goto out_err;
	}

	if (rados_connect(cl)) {
		printf("error connecting\n");
		goto out_err;
	}
	if (rados_connect(cl) == 0) {
		printf("second connect attempt didn't return an error\n");
		goto out_err;
	}

	/* create an io_ctx */
	r = rados_pool_create(cl, "foo");
	printf("rados_pool_create = %d\n", r);

	rados_ioctx_t io_ctx;
	r = rados_ioctx_create(cl, "foo", &io_ctx);
	if (r < 0) {
		printf("error creating ioctx\n");
		goto out_err;
	}
	printf("rados_ioctx_create = %d, io_ctx = %p\n", r, io_ctx);

	/* list all pools */
	{
		int buf_sz = rados_pool_list(cl, NULL, 0);
		printf("need buffer size of %d\n", buf_sz);
		char buf[buf_sz];
		int r = rados_pool_list(cl, buf, buf_sz);
		if (r != buf_sz) {
			printf("buffer size mismatch: got %d the first time, but %d "
			"the second.\n", buf_sz, r);
			goto out_err_cleanup;
		}
		const char *b = buf;
		printf("begin pools.\n");
		while (1) {
		if (b[0] == '\0')
		break;
		printf(" pool: '%s'\n", b);
		b += strlen(b) + 1;
		};
		printf("end pools.\n");
	}


	/* stat */
	struct rados_pool_stat_t st;
	r = rados_ioctx_pool_stat(io_ctx, &st);
	printf("rados_ioctx_pool_stat = %d, %lld KB, %lld objects\n", r, (long long)st.num_kb, (long long)st.num_objects);

	/* snapshots */
	r = rados_ioctx_snap_create(io_ctx, "snap1");
	printf("rados_ioctx_snap_create snap1 = %d\n", r);
	rados_snap_t snaps[10];
	r = rados_ioctx_snap_list(io_ctx, snaps, 10);
	for (i=0; i<r; i++) {
		char name[100];
		rados_ioctx_snap_get_name(io_ctx, snaps[i], name, sizeof(name));
		printf("rados_ioctx_snap_list got snap %lld %s\n", (long long)snaps[i], name);
	}
	rados_snap_t snapid;
	r = rados_ioctx_snap_lookup(io_ctx, "snap1", &snapid);
	printf("rados_ioctx_snap_lookup snap1 got %lld, result %d\n", (long long)snapid, r);
	r = rados_ioctx_snap_remove(io_ctx, "snap1");
	printf("rados_ioctx_snap_remove snap1 = %d\n", r);

	/* sync io */
	time_t tm;
	char buf[128], buf2[128];
	time(&tm);
	snprintf(buf, 128, "%s", ctime(&tm));
	const char *oid = "foo_object";
	r = rados_write(io_ctx, oid, buf, strlen(buf) + 1, 0);
	printf("rados_write = %d\n", r);
	r = rados_read(io_ctx, oid, buf2, sizeof(buf2), 0);
	printf("rados_read = %d\n", r);
	if (memcmp(buf, buf2, r))
		printf("*** content mismatch ***\n");

	/* attrs */
	if (do_rados_setxattr(io_ctx, oid, "b", "2"))
		goto out_err_cleanup;
	if (do_rados_setxattr(io_ctx, oid, "a", "1"))
		goto out_err_cleanup;
	if (do_rados_setxattr(io_ctx, oid, "c", "3"))
		goto out_err_cleanup;
	if (do_rados_getxattr(io_ctx, oid, "a", "1"))
		goto out_err_cleanup;
	if (do_rados_getxattr(io_ctx, oid, "b", "2"))
		goto out_err_cleanup;
	if (do_rados_getxattr(io_ctx, oid, "c", "3"))
		goto out_err_cleanup;
	const char *exkeys[] = { "a", "b", "c", NULL };
	const char *exvals[] = { "1", "2", "3", NULL };
	if (do_rados_getxattrs(io_ctx, oid, exkeys, exvals))
		goto out_err_cleanup;

	uint64_t size;
	time_t mtime;
	r = rados_stat(io_ctx, oid, &size, &mtime);
	printf("rados_stat size = %lld mtime = %d = %d\n", (long long)size, (int)mtime, r);
	r = rados_stat(io_ctx, "does_not_exist", NULL, NULL);
	printf("rados_stat(does_not_exist) = %d\n", r);

	/* exec */
	rados_exec(io_ctx, oid, "crypto", "md5", buf, strlen(buf) + 1, buf, 128);
	printf("exec result=%s\n", buf);
	r = rados_read(io_ctx, oid, buf2, 128, 0);
	printf("read result=%s\n", buf2);
	printf("size=%d\n", r);

	/* aio */
	rados_completion_t a, b;
	rados_aio_create_completion(0, 0, 0, &a);
	rados_aio_create_completion(0, 0, 0, &b);
	rados_aio_write(io_ctx, "a", a, buf, 100, 0);
	rados_aio_write(io_ctx, "../b/bb_bb_bb\\foo\\bar", b, buf, 100, 0);
	rados_aio_wait_for_safe(a);
	printf("a safe\n");
	rados_aio_wait_for_safe(b);
	printf("b safe\n");
	rados_aio_release(a);
	rados_aio_release(b);

	/* test flush */
	printf("testing aio flush\n");
	rados_completion_t c;
	rados_aio_create_completion(0, 0, 0, &c);
	rados_aio_write(io_ctx, "c", c, buf, 100, 0);
	int safe = rados_aio_is_safe(c);
	printf("a should not yet be safe and ... %s\n", safe ? "is":"is not");
	assert(!safe);
	rados_aio_flush(io_ctx);
	safe = rados_aio_is_safe(c);
	printf("a should be safe and ... %s\n", safe ? "is":"is not");
	assert(safe);
	rados_aio_release(c);
	
	rados_read(io_ctx, "../b/bb_bb_bb\\foo\\bar", buf2, 128, 0);

	/* list objects */
	rados_list_ctx_t h;
	r = rados_nobjects_list_open(io_ctx, &h);
	printf("rados_nobjects_list_open = %d, h = %p\n", r, h);
	const char *poolname;
	while (rados_nobjects_list_next(h, &poolname, NULL, NULL) == 0)
		printf("rados_nobjects_list_next got object '%s'\n", poolname);
	rados_nobjects_list_close(h);

	/* stat */
	r = rados_ioctx_pool_stat(io_ctx, &st);
	printf("rados_stat_pool = %d, %lld KB, %lld objects\n", r, (long long)st.num_kb, (long long)st.num_objects);

	ret = 0;

out_err_cleanup:
	/* delete a pool */
	rados_ioctx_destroy(io_ctx);

	r = rados_pool_delete(cl, "foo");
	printf("rados_delete_pool = %d\n", r);

out_err:
	rados_shutdown(cl);
	return ret;
}

int main(int argc, const char **argv)
{
	return testrados();
}
