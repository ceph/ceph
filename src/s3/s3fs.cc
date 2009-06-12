
#include <errno.h>
#include <stdlib.h>
#include <dirent.h>
#include <limits.h>

#include "s3access.h"

#include <string>
#include <vector>
#include <map>

using namespace std;

struct s3fs_state {
  DIR *dir;
};

int list_buckets_init(string& id, S3AccessHandle *handle)
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

int list_buckets_next(string& id, char *buf, int size, S3AccessHandle *handle)
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

int list_objects(string& id, string& bucket, int max, string& prefix, string& marker, vector<string>& result)
{
  map<string, bool> dir_map;
#define BUF_SIZE 512
  char path[BUF_SIZE];

  snprintf(path, BUF_SIZE, "/tmp/s3/%s", bucket.c_str());

  DIR *dir = opendir(path);
  if (!dir)
    return -errno;

  while (1) {
    struct dirent *dirent;
    dirent = readdir(dir);
    if (!dirent)
      break;
    if (dirent->d_name[0] == '.')
      continue;

    if (!prefix.empty()) {
      if (prefix.compare(0, prefix.size(), prefix) == 0)
        dir_map[dirent->d_name] = true;
    } else {
      dir_map[dirent->d_name] = true;
    }
  }

  closedir(dir);


  map<string, bool>::iterator iter;
  if (!marker.empty())
    iter = dir_map.lower_bound(marker);
  else
    iter = dir_map.begin();

  if (max < 0)
    max = INT_MAX;

  result.clear();
  int i;
  for (i=0; i<max && iter != dir_map.end(); i++, ++iter) {
    result.push_back(iter->first);
  }

  return i;
}


