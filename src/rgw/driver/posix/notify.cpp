// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "notify.h"
#ifdef linux
#include <sys/inotify.h>
#endif

namespace file::listing {

  std::unique_ptr<Notify> Notify::factory(Notifiable* n, const std::string& bucket_root, const std::string& notification_option)
  {
    if(notification_option == "RedisNotify") {
#if defined(BOOST_ASIO_HAS_CO_AWAIT)
      return std::unique_ptr<Notify>(new RedisNotify(n, bucket_root));
#endif
    } else {
#ifdef __linux__
      return std::unique_ptr<Notify>(new Inotify(n, bucket_root));
#else
#error currently, rgw posix driver requires inotify
#endif /* linux */
    }
    return nullptr;
  } /* Notify::factory */
}
