#include "cyan_store.h"

#include <fmt/format.h>

#include "common/safe_io.h"

void CyanStore::write_meta(const std::string& key,
                           const std::string& value)
{
  std::string v = value;
  v += "\n";
  if (int r = safe_write_file(path.c_str(), key.c_str(),
                              v.c_str(), v.length());
      r < 0) {
    throw std::runtime_error{fmt::format("unable to write_meta({})", key)};
  }
}

std::string CyanStore::read_meta(const std::string& key)
{
  char buf[4096];
  int r = safe_read_file(path.c_str(), key.c_str(),
                         buf, sizeof(buf));
  if (r <= 0)
    throw std::runtime_error{fmt::format("unable to read_meta({})", key)};
  // drop trailing newlines
  while (r && isspace(buf[r-1])) {
    --r;
  }
  return std::string(buf, r);
}
