// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <atomic>
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

    /* An event names a file, or a directory (`is_dir`), by its name in
     * the watched directory, which is `dir` under the bucket:  "" for the
     * bucket's own directory, otherwise a path ending in '/'.  Both views
     * live as long as the notify() call that receives them. */
    struct Event
    {
      EventType type;
      std::optional<std::string_view> name;
      std::string_view dir;
      bool is_dir{false};

      Event(EventType type, std::optional<std::string_view> name,
	    std::string_view dir = {}, bool is_dir = false) noexcept
	: type(type), name(name), dir(dir), is_dir(is_dir)
	{}

      Event(Event&& rhs) noexcept
	: type(rhs.type), name(rhs.name), dir(rhs.dir), is_dir(rhs.is_dir)
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
    friend class NullNotify;
  public:
    static std::unique_ptr<Notify> factory(Notifiable* n,
					   const std::string& bucket_root,
					   bool use_inotify);

    /* watch one directory of bucket `dname`:  `dir` under it, "" for the
     * bucket's own, otherwise ending in '/';  for names created, removed
     * and moved, and with `writes` for files written too */
    virtual int add_watch(const std::string& dname, const std::string& dir,
			  void* opaque, bool writes) = 0;
    int add_watch(const std::string& dname, void* opaque) {
      return add_watch(dname, std::string{}, opaque, false);
    }
    /* the watches of bucket `dname` on `dir` and the directories under
     * it, "" for all of them */
    virtual int remove_watch(const std::string& dname,
			     const std::string& dir) = 0;
    int remove_watch(const std::string& dname) {
      return remove_watch(dname, std::string{});
    }
    /* whether anything watches:  a caller need not gather directories
     * for a watcher that ignores them */
    virtual bool watching() const = 0;
    virtual ~Notify()
      {}
  }; /* Notify */

  class NullNotify : public Notify
  {
    NullNotify(Notifiable* n, const std::string& bucket_root)
      : Notify(n, bucket_root)
      {}

    friend class Notify;
  public:
    int add_watch(const std::string&, const std::string&, void*,
		  bool) override {
      return 0;
    }
    int remove_watch(const std::string&, const std::string&) override {
      return 0;
    }
    bool watching() const override { return false; }
  }; /* NullNotify */

#ifdef __linux__
  class Inotify : public Notify
  {
    static constexpr uint32_t rd_size = 8192;
    static constexpr uint32_t aw_mask = IN_ALL_EVENTS &
      ~(IN_MOVE_SELF|IN_OPEN|IN_ACCESS|IN_ATTRIB|IN_CLOSE_WRITE|IN_CLOSE_NOWRITE|IN_MODIFY|IN_DELETE_SELF);
    /* and a file written:  an overwrite in place changes no name.  Not
     * IN_CLOSE_WRITE, which closing any writable descriptor raises -- the
     * gateway opens files O_RDWR to read them, so building an entry would
     * raise the event that builds it again */
    static constexpr uint32_t aw_write_mask = aw_mask | IN_MODIFY;

    static constexpr uint64_t sig_shutdown = std::numeric_limits<uint64_t>::max() - 0xdeadbeef;

    class WatchRecord
    {
    public:
      int wd;
      std::string name;		/* the bucket */
      std::string dir;		/* the directory under it */
      void* opaque;
    public:
      WatchRecord(int wd, const std::string& name, const std::string& dir,
		  void* opaque) noexcept
	: wd(wd), name(name), dir(dir), opaque(opaque)
	{}

      WatchRecord(WatchRecord&& wr) noexcept
	: wd(wr.wd), name(std::move(wr.name)), dir(std::move(wr.dir)),
	  opaque(wr.opaque)
	{}

      WatchRecord& operator=(WatchRecord&& wr) {
	wd = wr.wd;
	name = std::move(wr.name);
	dir = std::move(wr.dir);
	opaque = wr.opaque;
	return *this;
      }
    }; /* WatchRecord */

    using wd_callback_map_t = ankerl::unordered_dense::map<int, WatchRecord>;

    int wfd, efd;
    std::mutex map_mutex;  // protects wd_callback_map
    wd_callback_map_t wd_callback_map;
    std::atomic<bool> shutdown{false};
    /* must be last: starting the thread (ev_loop) reaches every member
     * above, which are constructed in declaration order */
    std::thread thrd;

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
    
    /* the watch on `wp`, recorded;  -1 where it cannot be added */
    int watch(const std::string& name, const std::string& dir, void* opaque,
	      bool writes) {
      sf::path wp{rp / name};
      if (!dir.empty()) {
	wp /= dir;
      }
      int wd = inotify_add_watch(wfd, wp.c_str(),
				 writes ? aw_write_mask : aw_mask);
      if (wd == -1) {
	int err = errno;
	if (err != ENOENT) {
	  std::cerr << fmt::format("{} inotify_add_watch {} failed: {} ({})",
	    __func__, wp.string(), strerror(err), err) << std::endl;
	}
	return -1;
      }
      std::lock_guard lock(map_mutex);
      wd_callback_map.insert_or_assign(wd, WatchRecord(wd, name, dir, opaque));
      return wd;
    }

    /* drop the watches of `name`'s directories under `dir`, "" for all */
    void unwatch(const std::string& name, const std::string& dir) {
      std::lock_guard lock(map_mutex);
      for (auto it = wd_callback_map.begin(); it != wd_callback_map.end();) {
	if ((it->second.name == name) && it->second.dir.starts_with(dir)) {
	  (void) inotify_rm_watch(wfd, it->first);
	  it = wd_callback_map.erase(it);
	} else {
	  ++it;
	}
      }
    }

    void ev_loop() {
      std::unique_ptr<AlignedBuf> up_buf = std::make_unique<AlignedBuf>();
      struct inotify_event* event;
      char* buf = up_buf.get()->get();
      ssize_t len;
      int npoll;

      nfds_t nfds{2};
      struct pollfd fds[2] = {{wfd, POLLIN}, {efd, POLLIN}};

      while(! shutdown.load(std::memory_order_acquire)) {
	npoll = poll(fds, nfds, -1); /* for up to 10 fds, poll is fast as epoll */
	if (shutdown.load(std::memory_order_acquire)) {
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

	    if (event->mask & IN_Q_OVERFLOW) [[unlikely]] {
	      /* events were lost, for any watch:  every watched bucket's
	       * listing is invalid.  Its wd is -1, so it is not a watch's. */
	      std::vector<std::pair<std::string, void*>> buckets;
	      {
		std::lock_guard lock(map_mutex);
		for (const auto& [wd, wr] : wd_callback_map) {
		  if (wr.dir.empty()) {
		    buckets.emplace_back(wr.name, wr.opaque);
		  }
		}
	      }
	      for (const auto& [name, opaque] : buckets) {
		evec.clear();
		evec.emplace_back(Notifiable::Event(Notifiable::EventType::INVALIDATE,
						    std::nullopt));
		n->notify(name, opaque, evec);
	      }
	      break;	/* the rest of this read is no better */
	    }

	    /* copy the watch record while holding the lock */
	    std::string watch_name;
	    std::string watch_dir;
	    void* watch_opaque;
	    {
	      std::lock_guard lock(map_mutex);
	      const auto& it = wd_callback_map.find(event->wd);
	      if (it == wd_callback_map.end()) [[unlikely]] {
		/* non-destructive race, it happens */
		continue;
	      }
	      if (event->mask & IN_IGNORED) {
		/* the directory is gone, and its watch with it */
		wd_callback_map.erase(it);
		continue;
	      }
	      const auto& wr = it->second;
	      watch_name = wr.name;
	      watch_dir = wr.dir;
	      watch_opaque = wr.opaque;
	    }
	    if (event->len == 0) {
	      continue;
	    }
	    const std::string_view ename{event->name};
	    const bool is_dir = event->mask & IN_ISDIR;

	    if (event->mask & (IN_CREATE | IN_MOVED_TO | IN_MODIFY)) {
	      evec.emplace_back(Notifiable::Event(Notifiable::EventType::ADD,
						  ename, watch_dir, is_dir));
	    } else if (event->mask & (IN_DELETE | IN_MOVED_FROM)) {
	      evec.emplace_back(Notifiable::Event(Notifiable::EventType::REMOVE,
						  ename, watch_dir, is_dir));
	    }
	    if (evec.size() > 0) {
	      n->notify(watch_name, watch_opaque, evec);
	      evec.clear();
	    }
	  } /* events */
	} /* n > 0 */
      }
    } /* ev_loop */

    Inotify(Notifiable* n, const std::string& bucket_root)
      : Notify(n, bucket_root),
	wfd(inotify_init1(IN_NONBLOCK)),
	efd(eventfd(0, EFD_NONBLOCK)),
	thrd(&Inotify::ev_loop, this)
      {
	if (wfd == -1) {
	  std::cerr << fmt::format("{} inotify_init1 failed with {}", __func__, wfd) << std::endl;
	  exit(1);
	}
	if (efd == -1) {
	  std::cerr << fmt::format("{} eventfd failed", __func__) << std::endl;
	  exit(1);
	}
      }

    void signal_shutdown() {
      uint64_t msg{sig_shutdown};
      std::ignore = write(efd, &msg, sizeof(uint64_t));
    }

    friend class Notify;
  public:
    virtual int add_watch(const std::string& dname, const std::string& dir,
			  void* opaque, bool writes) override {
      return watch(dname, dir, opaque, writes);
    }

    virtual int remove_watch(const std::string& dname,
			     const std::string& dir) override {
      unwatch(dname, dir);
      return 0;
    }

    bool watching() const override { return true; }

    virtual ~Inotify() {
      shutdown.store(true, std::memory_order_release);
      signal_shutdown();
      thrd.join();
      close(wfd);
      close(efd);
    }
  };
#endif /* linux */

} // namespace file::listing
