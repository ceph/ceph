// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <cerrno>
#include <unordered_map>

#include "common/errno.h"
#include "common/safe_io.h" // for safe_read()
#include "common/split.h"

#include "driver/rados/rgw_tools.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

#define READ_CHUNK_LEN (512 * 1024)

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

int ext_mime_map_init(const DoutPrefixProvider *dpp, CephContext *cct, const char *ext_map)
{
  int fd = open(ext_map, O_RDONLY);
  char *buf = NULL;
  int ret;
  if (fd < 0) {
    ret = -errno;
    ldpp_dout(dpp, 0) << __func__ << " failed to open file=" << ext_map
                  << " : " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  struct stat st;
  ret = fstat(fd, &st);
  if (ret < 0) {
    ret = -errno;
    ldpp_dout(dpp, 0) << __func__ << " failed to stat file=" << ext_map
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
  if (ret != st.st_size) {
    // huh? file size has changed?
    ldpp_dout(dpp, 0) << __func__ << " raced! will retry.." << dendl;
    free(buf);
    close(fd);
    return ext_mime_map_init(dpp, cct, ext_map);
  }
  buf[st.st_size] = '\0';

  parse_mime_map(buf);
  ret = 0;
done:
  free(buf);
  close(fd);
  return ret;
}
} // namespace

std::string_view rgw_find_mime_by_ext(std::string_view ext)
{
  auto iter = ext_mime_map->find(ext);
  if (iter == ext_mime_map->end())
    return std::string_view{};

  return iter->second;
}

int rgw_tools_init(const DoutPrefixProvider *dpp, CephContext *cct)
{
  ext_mime_map = std::make_unique<std::map<std::string, std::string, std::less<>>>();
  ext_mime_map_init(dpp, cct, cct->_conf->rgw_mime_types_file.c_str());
  // ignore errors; missing mime.types is not fatal
  return 0;
}

void rgw_tools_cleanup()
{
  ext_mime_map = nullptr;
}
