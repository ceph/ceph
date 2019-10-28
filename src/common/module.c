/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "acconfig.h"
#include "include/compat.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#if defined(__FreeBSD__)
#include <sys/wait.h>
#endif 

/*
 * TODO: Switch to libkmod when we abandon older platforms.  The APIs
 * we want are:
 *
 * - kmod_module_new_from_name() for obtaining handles;
 * - kmod_module_probe_insert_module() for module_load();
 * - kmod_module_get_info(), kmod_module_info_get_{key,value}() for
 *   module_has_param().
 */

/*
 * Return command's exit status or -1 on error.
 */
static int run_command(const char *command)
{
	int status;

	status = system(command);
	if (status >= 0 && WIFEXITED(status))
		return WEXITSTATUS(status);

	if (status < 0) {
		char error_buf[80];
		char* errp = ceph_strerror_r(errno, error_buf, sizeof(error_buf));
		fprintf(stderr, "couldn't run '%s': %s\n", command,
			errp);
	} else if (WIFSIGNALED(status)) {
		fprintf(stderr, "'%s' killed by signal %d\n", command,
			WTERMSIG(status));
	} else {
		fprintf(stderr, "weird status from '%s': %d\n", command,
			status);
	}

	return -1;
}

int module_has_param(const char *module, const char *param)
{
	char command[128];

	snprintf(command, sizeof(command),
		 "/sbin/modinfo -F parm %s | /bin/grep -q ^%s:",
		 module, param);

	return run_command(command) == 0;
}

int module_load(const char *module, const char *options)
{
	char command[128];

	snprintf(command, sizeof(command), "/sbin/modprobe %s %s",
		 module, (options ? options : ""));

	return run_command(command);
}
