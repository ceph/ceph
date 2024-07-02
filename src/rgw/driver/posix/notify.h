// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <iostream>
#include <string>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <optional>
#include <filesystem>
#include <limits>
#include <cstdlib>
#include "unordered_dense.h"
#include <unistd.h>
#include <poll.h>
#ifdef __linux__
#include <sys/inotify.h>
#include <sys/eventfd.h>
#endif
#include <fmt/format.h>

namespace file::listing {

  namespace sf = std::filesystem;

  class Notifiable
  {
  public:
    enum class EventType : uint8_t
    {
      ADD = 0,
      REMOVE,
      INVALIDATE
    };

    struct Event
    {
      EventType type;
      std::optional<std::string_view> name;

      Event(EventType type, std::optional<std::string_view> name) noexcept
	: type(type), name(name)
	{}

      Event(Event&& rhs) noexcept
	: type(rhs.type), name(rhs.name)
	{}
    };

    virtual ~Notifiable() {};

    virtual int notify(const std::string&, void*, const std::vector<Event>&) = 0;
  };

  class Notify
  {
    Notifiable* n;
    sf::path rp;

    Notify(Notifiable* n, const std::string& bucket_root)
      : n(n), rp(bucket_root)
      {}

    friend class Inotify;
  public:
    static std::unique_ptr<Notify> factory(Notifiable* n, const std::string& bucket_root);
    
    virtual int add_watch(const std::string& dname, void* opaque) = 0;
    virtual int remove_watch(const std::string& dname) = 0;
    virtual ~Notify()
      {}
  }; /* Notify */

#ifdef __linux__
  class Inotify : public Notify
  {
    static constexpr uint32_t rd_size = 8192;
    static constexpr uint32_t aw_mask = IN_ALL_EVENTS &
      ~(IN_MOVE_SELF|IN_OPEN|IN_ACCESS|IN_ATTRIB|IN_CLOSE_WRITE|IN_CLOSE_NOWRITE|IN_MODIFY|IN_DELETE_SELF);

    static constexpr uint64_t sig_shutdown = std::numeric_limits<uint64_t>::max() - 0xdeadbeef;

    class WatchRecord
    {
    public:
      int wd;
      std::string name;
      void* opaque;
    public:
      WatchRecord(int wd, const std::string& name, void* opaque) noexcept
	: wd(wd), name(name), opaque(opaque)
	{}

      WatchRecord(WatchRecord&& wr) noexcept
	: wd(wr.wd), name(wr.name), opaque(wr.opaque)
	{}

      WatchRecord& operator=(WatchRecord&& wr) {
	wd = wr.wd;
	name = std::move(wr.name);
	opaque = wr.opaque;
	return *this;
      }
    }; /* WatchRecord */

    using wd_callback_map_t = ankerl::unordered_dense::map<int, WatchRecord>;
    using wd_remove_map_t = ankerl::unordered_dense::map<std::string, int>;

    int wfd, efd;
    std::thread thrd;
    wd_callback_map_t wd_callback_map;
    wd_remove_map_t wd_remove_map;
    bool shutdown{false};

    class AlignedBuf
    {
      char* m;
    public:
      AlignedBuf() {
	m = static_cast<char*>(aligned_alloc(__alignof__(struct inotify_event), rd_size));
	if (! m) [[unlikely]] {
	  std::cerr << fmt::format("{} buffer allocation failure", __func__) << std::endl;
	  abort();
	}
      }
      ~AlignedBuf() {
	std::free(m);
      }
      char* get() {
	return m;
      }
    }; /* AlignedBuf */
    
    void ev_loop() {
      std::unique_ptr<AlignedBuf> up_buf = std::make_unique<AlignedBuf>();
      struct inotify_event* event;
      char* buf = up_buf.get()->get();
      ssize_t len;
      int npoll;

      nfds_t nfds{2};
      struct pollfd fds[2] = {{wfd, POLLIN}, {efd, POLLIN}};

    restart:
      while(! shutdown) {
	npoll = poll(fds, nfds, -1); /* for up to 10 fds, poll is fast as epoll */
	if (shutdown) {
	  return;
	}
	if (npoll == -1) {
	  if (errno == EINTR) {
	    continue;
	  }
	  // XXX
	}
	if (npoll > 0) {
	  len = read(wfd, buf, rd_size);
	  if (len == -1) {
	    continue; // hopefully, was EAGAIN
	  }
	  std::vector<Notifiable::Event> evec;
	  for (char* ptr = buf; ptr < buf + len;
	       ptr += sizeof(struct inotify_event) + event->len) {
	    event = reinterpret_cast<struct inotify_event*>(ptr);
	    const auto& it = wd_callback_map.find(event->wd);
	    //std::cout << fmt::format("event! {}", event->name) << std::endl;
	    if (it == wd_callback_map.end()) [[unlikely]] {
	      /* non-destructive race, it happens */
	      continue;
	    }
	    const auto& wr = it->second;
	    if (event->mask & IN_Q_OVERFLOW) [[unlikely]] {
	      /* cache blown, invalidate */
	      evec.clear();
	      evec.emplace_back(Notifiable::Event(Notifiable::EventType::INVALIDATE, std::nullopt));
	      n->notify(wr.name, wr.opaque, evec);
	      goto restart;
	    } else {
	      if ((event->mask & IN_CREATE) ||
		  (event->mask & IN_MOVED_TO)) {
		/* new object in dir */
		evec.emplace_back(Notifiable::Event(Notifiable::EventType::ADD, event->name));
	      } else if ((event->mask & IN_DELETE) ||
			 (event->mask & IN_MOVED_FROM)) {
		/* object removed from dir */
		evec.emplace_back(Notifiable::Event(Notifiable::EventType::REMOVE, event->name));
	      }
	    } /* !overflow */
	    if (evec.size() > 0) {
	      n->notify(wr.name, wr.opaque, evec);
	    }
	  } /* events */
	} /* n > 0 */
      }
    } /* ev_loop */

    Inotify(Notifiable* n, const std::string& bucket_root)
      : Notify(n, bucket_root),
	thrd(&Inotify::ev_loop, this)
      {
	wfd = inotify_init1(IN_NONBLOCK);
	if (wfd == -1) {
	  std::cerr << fmt::format("{} inotify_init1 failed with {}", __func__, wfd) << std::endl;
	  exit(1);
	}
	efd = eventfd(0, EFD_NONBLOCK);
      }

    void signal_shutdown() {
      uint64_t msg{sig_shutdown};
      (void) write(efd, &msg, sizeof(uint64_t));
    }

    friend class Notify;
  public:
    virtual int add_watch(const std::string& dname, void* opaque) override {
      sf::path wp{rp / dname};
      int wd = inotify_add_watch(wfd, wp.c_str(), aw_mask);
      if (wd == -1) {
	std::cerr << fmt::format("{} inotify_add_watch {} failed with {}", __func__, dname, wd) << std::endl;
      } else {
	wd_callback_map.insert(wd_callback_map_t::value_type(wd, WatchRecord(wd, dname, opaque)));
	wd_remove_map.insert(wd_remove_map_t::value_type(dname, wd));
      }
      return wd;
    }

    virtual int remove_watch(const std::string& dname) override {
      int r{0};
      const auto& elt = wd_remove_map.find(dname);
      if (elt != wd_remove_map.end()) {
	auto& wd = elt->second;
	r = inotify_rm_watch(wfd, wd);
	if (r == -1) {
	  std::cerr << fmt::format("{} inotify_rm_watch {} failed with {}", __func__, dname, wd) << std::endl;
	}
	wd_callback_map.erase(wd);
	wd_remove_map.erase(std::string(dname));
      }
      return r;
    }

    virtual ~Inotify() {
      shutdown = true;
      signal_shutdown();
      thrd.join();
    }
  };
#endif /* linux */

} // namespace file::listing
