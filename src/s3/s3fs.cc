
#include <errno.h>
#include <stdlib.h>
#include <dirent.h>
#include <limits.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "s3access.h"

#include <string>
#include <vector>
#include <map>

using namespace std;

struct s3fs_state {
  DIR *dir;
};

#define DIR_NAME "/tmp/s3"

int list_buckets_init(string& id, S3AccessHandle *handle)
{
  DIR *dir = opendir(DIR_NAME);
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

int list_buckets_next(string& id, S3ObjEnt& obj, S3AccessHandle *handle)
{
  struct s3fs_state *state;
  struct dirent *dirent;
#define BUF_SIZE 512

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

    obj.name = dirent->d_name;

    char buf[BUF_SIZE];
    struct stat statbuf;
    snprintf(buf, BUF_SIZE, "%s/%s", DIR_NAME, obj.name.c_str());
    if (stat(buf, &statbuf) < 0)
      continue;
    obj.mtime = statbuf.st_mtime;
    obj.size = statbuf.st_size;
    return 0;
  }
}

int list_objects(string& id, string& bucket, int max, string& prefix, string& marker, vector<S3ObjEnt>& result)
{
  map<string, bool> dir_map;
  char path[BUF_SIZE];

  snprintf(path, BUF_SIZE, "%s/%s", DIR_NAME, bucket.c_str());

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
    S3ObjEnt obj;
    char buf[BUF_SIZE];
    struct stat statbuf;
    obj.name = iter->first;
    snprintf(buf, BUF_SIZE, "%s/%s", path, obj.name.c_str());
    if (stat(buf, &statbuf) < 0)
      continue;
    obj.mtime = statbuf.st_mtime;
    obj.size = statbuf.st_size;
    result.push_back(obj);
  }

  return i;
}


int create_bucket(std::string& id, std::string& bucket)
{
  int len = strlen(DIR_NAME) + 1 + bucket.size() + 1;
  char buf[len];
  snprintf(buf, len, "%s/%s", DIR_NAME, bucket.c_str());

  if (mkdir(buf, 0755) < 0)
    return -errno;

  return 0;
}


int put_obj(std::string& id, std::string& bucket, std::string& obj, const char *data, size_t size)
{
  int len = strlen(DIR_NAME) + 1 + bucket.size() + 1 + obj.size() + 1;
  char buf[len];
  snprintf(buf, len, "%s/%s/%s", DIR_NAME, bucket.c_str(), obj.c_str());
  int fd;

  fd = open(buf, O_CREAT | O_APPEND | O_WRONLY, 0755);
  if (fd < 0)
    return -errno;

  int r = write(fd, data, size);
  if (r < 0) {
    r = -errno;
    close(fd);
    unlink(buf);
    return r;
  }

  r = close(r);
  if (r < 0)
    return -errno;

  return 0;
}
