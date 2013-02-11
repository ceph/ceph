/*
 * Copyright (C) 2010 Canonical
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
 * 
 */

/*
 *  Author Colin Ian King,  colin.king@canonical.com
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>

#if defined(__linux__)
#include <linux/fs.h>
#endif
#include "include/inttypes.h"
#include "include/fiemap.h"

struct fiemap *read_fiemap(int fd)
{
  struct fiemap *fiemap;
  struct fiemap *_realloc_fiemap = NULL;
  int extents_size;
  int r;

  if ((fiemap = (struct fiemap*)malloc(sizeof(struct fiemap))) == NULL) {
    fprintf(stderr, "Out of memory allocating fiemap\n");
    return NULL;
  }
  memset(fiemap, 0, sizeof(struct fiemap));

  fiemap->fm_start = 0;
  fiemap->fm_length = ~0;		/* Lazy */
  fiemap->fm_flags = 0;
  fiemap->fm_extent_count = 0;
  fiemap->fm_mapped_extents = 0;

  /* Find out how many extents there are */
  r = ioctl(fd, FS_IOC_FIEMAP, fiemap);
  if (r < 0) {
    goto done_err;
  }

  if (!fiemap->fm_mapped_extents) {
    goto done_err;
  }

  /* Read in the extents */
  extents_size = sizeof(struct fiemap_extent) * (fiemap->fm_mapped_extents);

  /* Resize fiemap to allow us to read in the extents */

  if ((_realloc_fiemap = (struct fiemap*)realloc(fiemap,sizeof(struct fiemap) +
                                        extents_size)) == NULL) {
    fprintf(stderr, "Out of memory allocating fiemap\n");
    goto done_err;
  } else {
    fiemap = _realloc_fiemap;
  }

  memset(fiemap->fm_extents, 0, extents_size);
  fiemap->fm_extent_count = fiemap->fm_mapped_extents;
  fiemap->fm_mapped_extents = 0;

  if (ioctl(fd, FS_IOC_FIEMAP, fiemap) < 0) {
    fprintf(stderr, "fiemap ioctl() failed\n");
    goto done_err;
  }

  return fiemap;
done_err:
  free(fiemap);
  return NULL;
}

