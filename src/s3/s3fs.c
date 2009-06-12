
#include <errno.h>
#include <stdlib.h>
#include <dirent.h>

#include "s3access.h"

struct s3fs_state {
  DIR *dir;
};

int list_buckets_init(const char *id, S3AccessHandle *handle)
{
  DIR *dir = opendir("/tmp/s3");
  struct s3fs_state *state;

  if (!dir)
    return -errno;

  state = (struct s3fs_state *)malloc(sizeof(struct s3fs_state));
  if (!state)
    return -ENOMEM;

  state->dir = dir;

  *handle = (S3AccessHandle)state;

  return 0;
}

int list_buckets_next(const char *id, char *buf, int size, S3AccessHandle *handle)
{
  struct s3fs_state *state;
  struct dirent *dirent;

  if (!handle)
    return -EINVAL;
  state = *(struct s3fs_state **)handle;

  if (!state)
    return -EINVAL;

  while (1) {
    dirent = readdir(state->dir);
    if (!dirent) {
      closedir(state->dir);
      *handle = NULL;
      return -ENOENT;
    }

    if (dirent->d_name[0] == '.')
      continue;

    snprintf(buf, size, "%s", dirent->d_name);
    return 0;
  }
}


