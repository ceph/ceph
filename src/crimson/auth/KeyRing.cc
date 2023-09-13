// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "KeyRing.h"

#include <boost/algorithm/string.hpp>

#include <seastar/core/do_with.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/reactor.hh>

#include "common/buffer_seastar.h"
#include "auth/KeyRing.h"
#include "include/denc.h"
#include "crimson/common/buffer_io.h"
#include "crimson/common/config_proxy.h"

namespace crimson::auth {

seastar::future<KeyRing*> load_from_keyring(KeyRing* keyring)
{
  std::vector<std::string> paths;
  boost::split(paths, crimson::common::local_conf()->keyring,
               boost::is_any_of(",;"));
  std::pair<bool, std::string> found;
  return seastar::map_reduce(paths, [](auto path) {
    return seastar::engine().file_exists(path).then([path](bool file_exists) {
      return std::make_pair(file_exists, path);
    });
  }, std::move(found), [](auto found, auto file_exists_and_path) {
    if (!found.first && file_exists_and_path.first) {
      found = std::move(file_exists_and_path);
    }
    return found;
  }).then([keyring] (auto file_exists_and_path) {
    const auto& [exists, path] = file_exists_and_path;
    if (exists) {
      return read_file(path).then([keyring](auto buf) {
        bufferlist bl;
        bl.append(buffer::create(std::move(buf)));
        auto i = bl.cbegin();
        keyring->decode(i);
        return seastar::make_ready_future<KeyRing*>(keyring);
      });
    } else {
      return seastar::make_ready_future<KeyRing*>(keyring);
    }
  });
}

seastar::future<KeyRing*> load_from_keyfile(KeyRing* keyring)
{
  auto& path = crimson::common::local_conf()->keyfile;
  if (!path.empty()) {
    return read_file(path).then([keyring](auto buf) {
      EntityAuth ea;
      ea.key.decode_base64(std::string(buf.begin(),
                                       buf.end()));
      keyring->add(crimson::common::local_conf()->name, ea);
      return seastar::make_ready_future<KeyRing*>(keyring);
    });
  } else {
    return seastar::make_ready_future<KeyRing*>(keyring);
  }
}

seastar::future<KeyRing*> load_from_key(KeyRing* keyring)
{
  auto& key = crimson::common::local_conf()->key;
  if (!key.empty()) {
    EntityAuth ea;
    ea.key.decode_base64(key);
    keyring->add(crimson::common::local_conf()->name, ea);
  }
  return seastar::make_ready_future<KeyRing*>(keyring);
}

} // namespace crimson::auth
