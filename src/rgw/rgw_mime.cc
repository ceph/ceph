// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <map>
#include <memory>
#include <string>

#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "common/split.h"

#include "rgw_mime.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

using namespace std;

namespace {

std::unique_ptr<std::map<std::string, std::string, std::less<>>> ext_mime_map;

void parse_mime_map_line(std::string_view line)
{
  static constexpr std::string_view delims{" \t\n\r"};
  static constexpr std::string_view wschars{" \f\n\r\t\v"};
  auto spaces = line.find_first_not_of(wschars);
  if (spaces == line.npos) {
    return;
  }
  line.remove_prefix(spaces);

  auto splitline = ceph::split(line, delims);
  auto iter = splitline.begin();
  if (iter == splitline.end()) {
    return;
  }
  auto mime = *iter;
  ++iter;
  while (iter != splitline.end()) {
    (*ext_mime_map)[std::string{*iter}] = mime;
    ++iter;
  }
}

void parse_mime_map(const char *buf)
{
  const char *start = buf, *end = buf;
  while (*end) {
    while (*end && *end != '\n') {
      end++;
    }
    parse_mime_map_line({start, end});
    end++;
    start = end;
  }
}

int load_mime_file(const DoutPrefixProvider *dpp, const char *path)
{
  int fd = open(path, O_RDONLY);
  char *buf = NULL;
  int ret;
  if (fd < 0) {
    ret = -errno;
    ldpp_dout(dpp, 0) << __func__ << " failed to open file=" << path
                  << " : " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  struct stat st;
  ret = fstat(fd, &st);
  if (ret < 0) {
    ret = -errno;
    ldpp_dout(dpp, 0) << __func__ << " failed to stat file=" << path
                  << " : " << cpp_strerror(-ret) << dendl;
    goto done;
  }

  buf = (char *)malloc(st.st_size + 1);
  if (!buf) {
    ret = -ENOMEM;
    ldpp_dout(dpp, 0) << __func__ << " failed to allocate buf" << dendl;
    goto done;
  }

  ret = safe_read(fd, buf, st.st_size + 1);
  if (ret < 0) {
    goto done;
  }
  buf[ret] = '\0';

  parse_mime_map(buf);
  ret = 0;
done:
  free(buf);
  close(fd);
  return ret;
}

} // namespace

int rgw_mime_init(const DoutPrefixProvider *dpp, CephContext *cct)
{
  ext_mime_map = std::make_unique<std::map<std::string, std::string, std::less<>>>();
  load_mime_file(dpp, cct->_conf->rgw_mime_types_file.c_str());
  return 0;
}

void rgw_mime_cleanup()
{
  ext_mime_map = nullptr;
}

std::string_view rgw_find_mime_by_ext(std::string_view ext)
{
  if (!ext_mime_map)
    return std::string_view{};
  auto iter = ext_mime_map->find(ext);
  if (iter == ext_mime_map->end())
    return std::string_view{};
  return iter->second;
}
