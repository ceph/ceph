/*
 * canonicalize.c -- canonicalize pathname by removing symlinks
 * Copyright (C) 1993 Rick Sladkey <jrs@world.std.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library Public License as published by
 * the Free Software Foundation; either version 2, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library Public License for more details.
 *
 */

/*
 * This routine is part of libc.  We include it nevertheless,
 * since the libc version has some security flaws.
 *
 * TODO: use canonicalize_file_name() when exist in glibc
 */
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <limits.h>

#ifndef MAXSYMLINKS
# define MAXSYMLINKS 256
#endif

static char *
myrealpath(const char *path, char *resolved_path, int maxreslth) {
	int readlinks = 0;
	char *npath;
	char *link_path;
	int n;
	char *buf = NULL;

	npath = resolved_path;

	/* If it's a relative pathname use getcwd for starters. */
	if (*path != '/') {
		if (!getcwd(npath, maxreslth-2))
			return NULL;
		npath += strlen(npath);
		if (npath[-1] != '/')
			*npath++ = '/';
	} else {
		*npath++ = '/';
		path++;
	}

	/* Expand each slash-separated pathname component. */
	link_path = malloc(PATH_MAX+1);
	if (!link_path)
		return NULL;
	while (*path != '\0') {
		/* Ignore stray "/" */
		if (*path == '/') {
			path++;
			continue;
		}
		if (*path == '.' && (path[1] == '\0' || path[1] == '/')) {
			/* Ignore "." */
			path++;
			continue;
		}
		if (*path == '.' && path[1] == '.' &&
		    (path[2] == '\0' || path[2] == '/')) {
			/* Backup for ".." */
			path += 2;
			while (npath > resolved_path+1 &&
			       (--npath)[-1] != '/')
				;
			continue;
		}
		/* Safely copy the next pathname component. */
		while (*path != '\0' && *path != '/') {
			if (npath-resolved_path > maxreslth-2) {
				errno = ENAMETOOLONG;
				goto err;
			}
			*npath++ = *path++;
		}

		/* Protect against infinite loops. */
		if (readlinks++ > MAXSYMLINKS) {
			errno = ELOOP;
			goto err;
		}

		/* See if last pathname component is a symlink. */
		*npath = '\0';

		n = readlink(resolved_path, link_path, PATH_MAX);
		if (n < 0) {
			/* EINVAL means the file exists but isn't a symlink. */
			if (errno != EINVAL)
				goto err;
		} else {
			int m;
			char *newbuf;

			/* Note: readlink doesn't add the null byte. */
			link_path[n] = '\0';
			if (*link_path == '/')
				/* Start over for an absolute symlink. */
				npath = resolved_path;
			else
				/* Otherwise back up over this component. */
				while (*(--npath) != '/')
					;

			/* Insert symlink contents into path. */
			m = strlen(path);
			newbuf = malloc(m + n + 1);
			if (!newbuf)
				goto err;
			memcpy(newbuf, link_path, n);
			memcpy(newbuf + n, path, m + 1);
			free(buf);
			path = buf = newbuf;
		}
		*npath++ = '/';
	}
	/* Delete trailing slash but don't whomp a lone slash. */
	if (npath != resolved_path+1 && npath[-1] == '/')
		npath--;
	/* Make sure it's null terminated. */
	*npath = '\0';

	free(link_path);
	free(buf);
	return resolved_path;

 err:
	free(link_path);
	free(buf);
	return NULL;
}

/*
 * Converts private "dm-N" names to "/dev/mapper/<name>"
 *
 * Since 2.6.29 (patch 784aae735d9b0bba3f8b9faef4c8b30df3bf0128) kernel sysfs
 * provides the real DM device names in /sys/block/<ptname>/dm/name
 */
char *
canonicalize_dm_name(const char *ptname)
{
	FILE	*f;
	size_t	sz;
	char	path[256], name[256], *res = NULL;

	snprintf(path, sizeof(path), "/sys/block/%s/dm/name", ptname);
	if (!(f = fopen(path, "r")))
		return NULL;

	/* read "<name>\n" from sysfs */
	if (fgets(name, sizeof(name), f) && (sz = strlen(name)) > 1) {
		name[sz - 1] = '\0';
		snprintf(path, sizeof(path), "/dev/mapper/%s", name);
		res = strdup(path);
	}
	fclose(f);
	return res;
}

char *
canonicalize_path(const char *path)
{
	char *canonical;
	char *p;

	if (path == NULL)
		return NULL;

	canonical = malloc(PATH_MAX+2);
	if (!canonical)
		return NULL;
	if (!myrealpath(path, canonical, PATH_MAX+1)) {
		free(canonical);
		return strdup(path);
	}


	p = strrchr(canonical, '/');
	if (p && strncmp(p, "/dm-", 4) == 0 && isdigit(*(p + 4))) {
		p = canonicalize_dm_name(p+1);
		if (p) {
			free(canonical);
			return p;
		}
	}

	return canonical;
}


