#include "include/rados/librados.h"
#include "test/rados-api/test.h"

#include <stdlib.h>
#include <string>
#include <time.h>

std::string get_temp_pool_name()
{
  char out[17];
  memset(out, 0, sizeof(out));
  srand(time(NULL));
  for (size_t i = 0; i < sizeof(out) - 1; ++i) {
    out[i] = 'A' + (rand() % 26);
  }
  return out;
}

int create_one_pool(const std::string &pool_name, rados_t *cluster)
{
  int ret;
  ret = rados_create(cluster, NULL);
  if (ret)
    return ret;
  ret = rados_conf_read_file(*cluster, NULL);
  if (ret) {
    rados_shutdown(*cluster);
    return ret;
  }
  ret = rados_connect(*cluster);
  if (ret) {
    rados_shutdown(*cluster);
    return ret;
  }
  ret = rados_pool_create(*cluster, pool_name.c_str());
  if (ret) {
    rados_shutdown(*cluster);
    return ret;
  }
  return 0;
}

int destroy_one_pool(const std::string &pool_name, rados_t *cluster)
{
  int ret = rados_pool_delete(*cluster, pool_name.c_str());
  if (ret) {
    rados_shutdown(*cluster);
    return ret;
  }
  rados_shutdown(*cluster);
  return 0;
}
