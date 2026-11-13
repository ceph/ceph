// Check one existing bucket-index shard; optionally publish its calculated statistics.
#include "cls/rgw/cls_rgw_client.h"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <new>
#include <string>

static int maintain(librados::IoCtx& io, const std::string& oid, const bool rebuild)
{
  // begin-page-loop
  rgw_cls_index_check check;
  while (!check.complete()) {
    ceph::bufferlist page;
    librados::ObjectReadOperation read;
    cls_rgw_bucket_check_index(read, check.cursor, page);
    if (const int result = io.operate(oid, &read, nullptr); result < 0) {
      return result;
    }
    if (const int result = check.consume(page); result < 0) {
      return result;
    }
  }
  // end-page-loop
  for (const auto& [category, stats] : check.result.calculated_header.stats) {
    std::printf("category=%u entries=%llu bytes=%llu\n", static_cast<unsigned>(category),
                static_cast<unsigned long long>(stats.num_entries),
                static_cast<unsigned long long>(stats.total_size));
  }
  if (!rebuild) {
    return 0;
  }
  librados::ObjectWriteOperation write;
  cls_rgw_bucket_rebuild_index(write, check);
  return io.operate(oid, &write);
}

int main(int argc, char** argv)
try {
  if (argc < 4 || argc > 5 || (argc == 5 && std::strcmp(argv[4], "--rebuild") != 0)) {
    std::fprintf(stderr, "usage: %s CONFIG POOL INDEX_OBJECT [--rebuild]\n", argv[0]);
    return 2;
  }
  librados::Rados cluster;
  int result = cluster.init2("client.admin", "ceph", 0);
  if (result >= 0) {
    result = cluster.conf_read_file(argv[1]);
  }
  if (result >= 0) {
    result = cluster.connect();
  }
  librados::IoCtx io;
  if (result >= 0) {
    result = cluster.ioctx_create(argv[2], io);
  }
  if (result >= 0) {
    result = maintain(io, argv[3], argc == 5);
  }
  if (result < 0) {
    std::fprintf(stderr, "maintenance: %s (%d)\n", std::strerror(-result), result);
    if (result == -ECANCELED) {
      std::fprintf(stderr, "The shard changed; start a new scan.\n");
    }
    if (result == -EOPNOTSUPP) {
      std::fprintf(stderr, "The installed CLS module does not support paged maintenance.\n");
    }
    return 1;
  }
  return 0;
} catch (const std::bad_alloc&) {
  std::fprintf(stderr, "maintenance: allocation failed\n");
  return 1;
}
