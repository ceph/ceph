// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-

/**
 * crimson-store-nbd
 *
 * This tool exposes crimson object store internals as an nbd server
 * for use with fio in basic benchmarking.
 *
 * Example usage:
 *
 *  $ ./bin/crimson-store-nbd --device-path /dev/nvme1n1 -c 1 --total-device-size=107374182400 --mkfs true --uds-path /tmp/store_nbd_socket.sock
 *
 *  $ cat nbd.fio
 *  [global]
 *  ioengine=nbd
 *  uri=nbd+unix:///?socket=/tmp/store_nbd_socket.sock
 *  rw=randrw
 *  time_based
 *  runtime=120
 *  group_reporting
 *  iodepth=1
 *  size=500G
 *
 *  [job0]
 *  offset=0
 *
 *  $ fio nbd.fio
 */

#include <random>

#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

#include <linux/nbd.h>
#include <linux/fs.h>

#include <seastar/core/byteorder.hh>
#include <seastar/core/rwlock.hh>

#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/segment_cleaner.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/segment_manager/block.h"
#include "crimson/os/seastore/transaction_manager.h"

#include "test/crimson/seastar_runner.h"
#include "test/crimson/seastore/test_block.h"

namespace po = boost::program_options;

using namespace ceph;
using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;
using namespace crimson::os::seastore::segment_manager::block;

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}

/**
 * BlockDriver
 *
 * Simple interface to enable throughput test to compare raw disk to
 * transaction_manager, etc
 */
class BlockDriver {
public:
  struct config_t {
    std::string type;
    bool mkfs = false;
    std::optional<std::string> path;
    size_t segment_size;
    size_t total_device_size;

    void populate_options(
      po::options_description &desc)
    {
      desc.add_options()
	("type",
	 po::value<std::string>()
	 ->default_value("transaction_manager")
	 ->notifier([this](auto s) { type = s; }),
	 "Backend to use, options are transaction_manager"
	)
	("segment-size",
	 po::value<size_t>()
	 ->default_value(16ul << 20 /* 16MB */)
	 ->notifier([this](auto s) { segment_size = s; }),
	 "Total working set size"
	)
	("total-device-size",
	 po::value<size_t>()
	 ->default_value(10ul << 30 /* 10G */)
	 ->notifier([this](auto s) { total_device_size = s; }),
	 "Size of writes"
	)
	("device-path",
	 po::value<std::string>()
	 ->required()
	 ->notifier([this](auto s) { path = s; }),
	 "Number of writes outstanding"
	)
	("mkfs",
	 po::value<bool>()
	 ->default_value(false)
	 ->notifier([this](auto s) { mkfs = s; }),
	 "Do mkfs first"
	);
    }
  };

  virtual bufferptr get_buffer(size_t size) = 0;

  virtual seastar::future<> write(
    off_t offset,
    bufferptr ptr) = 0;

  virtual seastar::future<bufferlist> read(
    off_t offset,
    size_t size) = 0;

  virtual size_t get_size() const = 0;

  virtual seastar::future<> mount() = 0;
  virtual seastar::future<> close() = 0;

  virtual ~BlockDriver() {}
};
using BlockDriverRef = std::unique_ptr<BlockDriver>;

BlockDriverRef get_backend(BlockDriver::config_t config);

struct request_context_t {
  uint32_t magic = 0;
  uint32_t type = 0;

  char handle[8] = {0};

  uint64_t from = 0;
  uint32_t len = 0;

  unsigned err = 0;
  std::optional<bufferptr> in_buffer;
  std::optional<bufferlist> out_buffer;

  using ref = std::unique_ptr<request_context_t>;
  static ref make_ref() {
    return std::make_unique<request_context_t>();
  }

  bool check_magic() const {
    auto ret = magic == NBD_REQUEST_MAGIC;
    if (!ret) {
      logger().error(
	"Invalid magic {} should be {}",
	magic,
	NBD_REQUEST_MAGIC);
    }
    return ret;
  }

  uint32_t get_command() const {
    return type & 0xff;
  }

  bool has_input_buffer() const {
    return get_command() == NBD_CMD_WRITE;
  }

  seastar::future<> read_request(seastar::input_stream<char> &in) {
    return in.read_exactly(sizeof(struct nbd_request)
    ).then([this, &in](auto buf) {
      if (buf.size() < sizeof(struct nbd_request)) {
	throw std::system_error(
	  std::make_error_code(
	    std::errc::connection_reset));
      }
      auto p = buf.get();
      magic = seastar::consume_be<uint32_t>(p);
      type = seastar::consume_be<uint32_t>(p);
      memcpy(handle, p, sizeof(handle));
      p += sizeof(handle);
      from = seastar::consume_be<uint64_t>(p);
      len = seastar::consume_be<uint32_t>(p);
      logger().debug(
        "Got request, magic {}, type {}, from {}, len {}",
	magic, type, from, len);

      if (!check_magic()) {
       throw std::system_error(
	 std::make_error_code(
	   std::errc::invalid_argument));
      }

      if (has_input_buffer()) {
	return in.read_exactly(len).then([this](auto buf) {
	  in_buffer = ceph::buffer::create_page_aligned(len);
	  in_buffer->copy_in(0, len, buf.get());
	  return seastar::now();
	});
      } else {
	return seastar::now();
      }
    });
  }

  seastar::future<> write_reply(seastar::output_stream<char> &out) {
    seastar::temporary_buffer<char> buffer{sizeof(struct nbd_reply)};
    auto p = buffer.get_write();
    seastar::produce_be<uint32_t>(p, NBD_REPLY_MAGIC);
    seastar::produce_be<uint32_t>(p, err);
    logger().debug("write_reply writing err {}", err);
    memcpy(p, handle, sizeof(handle));
    return out.write(std::move(buffer)).then([this, &out] {
      if (out_buffer) {
        return seastar::do_for_each(
          out_buffer->mut_buffers(),
          [&out](bufferptr &ptr) {
	    logger().debug("write_reply writing {}", ptr.length());
            return out.write(
	      seastar::temporary_buffer<char>(
		ptr.c_str(),
		ptr.length(),
		seastar::make_deleter([ptr](){}))
	    );
          });
      } else {
        return seastar::now();
      }
    }).then([&out] {
      return out.flush();
    });
  }
};

struct RequestWriter {
  seastar::rwlock lock;
  seastar::output_stream<char> stream;
  bool stopped = false;
  int pending = 0;

  RequestWriter(
    seastar::output_stream<char> &&stream) : stream(std::move(stream)) {}
  RequestWriter(RequestWriter &&) = default;

  seastar::future<> complete(request_context_t::ref &&req) {
    if (stopped)
      throw std::system_error(
	std::make_error_code(
	  std::errc::operation_canceled));
    auto &request = *req;
    ++pending;
    return lock.write_lock(
    ).then([&request, this] {
      return request.write_reply(stream);
    }).finally([&, this, req=std::move(req)] {
      --pending;
      lock.write_unlock();
      logger().debug("complete");
      return seastar::now();
    });
  }

  seastar::future<> close() {
    stopped = true;
    return lock.write_lock(
    ).then([this] {
      assert(pending == 0);
      return stream.close();
    });
  }
};

/**
 * NBDHandler
 *
 * Simple throughput test for concurrent, single threaded
 * writes to an BlockDriver.
 */
class NBDHandler {
  BlockDriver &backend;
  std::string uds_path;
public:
  struct config_t {
    std::string uds_path;

    void populate_options(
      po::options_description &desc)
    {
      desc.add_options()
	("uds-path",
	 po::value<std::string>()
	 ->default_value("/tmp/store_nbd_socket.sock")
	 ->notifier([this](auto s) {
	   uds_path = s;
	 }),
	 "Path to domain socket for nbd"
	);
    }
  };

  NBDHandler(
    BlockDriver &backend,
    config_t config) :
    backend(backend),
    uds_path(config.uds_path)
  {}

  seastar::future<> run();
};

int main(int argc, char** argv)
{
  po::options_description desc{"Allowed options"};
  bool debug = false;
  desc.add_options()
    ("help,h", "show help message")
    ("debug", po::value<bool>(&debug)->default_value(false),
     "enable debugging");

  po::options_description nbd_pattern_options{"NBD Pattern Options"};
  NBDHandler::config_t nbd_config;
  nbd_config.populate_options(nbd_pattern_options);
  desc.add(nbd_pattern_options);

  po::options_description backend_pattern_options{"Backend Options"};
  BlockDriver::config_t backend_config;
  backend_config.populate_options(backend_pattern_options);
  desc.add(backend_pattern_options);

  po::variables_map vm;
  std::vector<std::string> unrecognized_options;
  try {
    auto parsed = po::command_line_parser(argc, argv)
      .options(desc)
      .allow_unregistered()
      .run();
    po::store(parsed, vm);
    if (vm.count("help")) {
      std::cout << desc << std::endl;
      return 0;
    }

    po::notify(vm);
    unrecognized_options =
      po::collect_unrecognized(parsed.options, po::include_positional);
 }  catch(const po::error& e) {
    std::cerr << "error: " << e.what() << std::endl;
    return 1;
  }
  std::vector<const char*> args(argv, argv + argc);

  seastar::app_template app;

  std::vector<char*> av{argv[0]};
  std::transform(begin(unrecognized_options),
                 end(unrecognized_options),
                 std::back_inserter(av),
                 [](auto& s) {
                   return const_cast<char*>(s.c_str());
                 });

  SeastarRunner sc;
  sc.init(av.size(), av.data());

  if (debug) {
    seastar::global_logger_registry().set_all_loggers_level(
      seastar::log_level::debug
    );
  }

  sc.run([=] {
    auto backend = get_backend(backend_config);
    return seastar::do_with(
      NBDHandler(*backend, nbd_config),
      std::move(backend),
      [](auto &nbd, auto &backend) {
	return backend->mount(
	).then([&] {
	  logger().debug("Running nbd server...");
	  return nbd.run();
	}).then([&] {
	  return backend->close();
	});
      });
  });
  sc.stop();
}

class nbd_oldstyle_negotiation_t {
  uint64_t magic = seastar::cpu_to_be(0x4e42444d41474943); // "NBDMAGIC"
  uint64_t magic2 = seastar::cpu_to_be(0x00420281861253);  // "IHAVEOPT"
  uint64_t size = 0;
  uint32_t flags = seastar::cpu_to_be(0);
  char reserved[124] = {0};

public:
  nbd_oldstyle_negotiation_t(uint64_t size, uint32_t flags)
    : size(seastar::cpu_to_be(size)), flags(seastar::cpu_to_be(flags)) {}
} __attribute__((packed));

seastar::future<> send_negotiation(
  size_t size,
  seastar::output_stream<char>& out)
{
  seastar::temporary_buffer<char> buf{sizeof(nbd_oldstyle_negotiation_t)};
  new (buf.get_write()) nbd_oldstyle_negotiation_t(size, 1);
  return out.write(std::move(buf)
  ).then([&out] {
    return out.flush();
  });
}

seastar::future<> handle_command(
  BlockDriver &backend,
  request_context_t::ref request_ref,
  RequestWriter &out)
{
  auto &request = *request_ref;
  logger().debug("got command {}", request.get_command());
  return ([&] {
    switch (request.get_command()) {
    case NBD_CMD_WRITE:
      return backend.write(
	request.from,
	*request.in_buffer);
    case NBD_CMD_READ:
      return backend.read(
	request.from,
	request.len).then([&] (auto buffer) {
	  logger().debug("read returned buffer len {}", buffer.length());
	  request.out_buffer = buffer;
	});
    case NBD_CMD_DISC:
      throw std::system_error(std::make_error_code(std::errc::bad_message));
    case NBD_CMD_TRIM:
      throw std::system_error(std::make_error_code(std::errc::bad_message));
    default:
      throw std::system_error(std::make_error_code(std::errc::bad_message));
    }
  })().then([&, request_ref=std::move(request_ref)]() mutable {
    logger().debug("handle_command complete");
    return out.complete(std::move(request_ref));
  });
}


seastar::future<> handle_commands(
  BlockDriver &backend,
  seastar::input_stream<char>& in,
  RequestWriter &out)
{
  logger().debug("handle_commands");
  return seastar::keep_doing(
    [&] {
      logger().debug("waiting for command");
      auto request_ref = request_context_t::make_ref();
      auto &request = *request_ref;
      return request.read_request(in
      ).then([&, request_ref=std::move(request_ref)]() mutable {
	static_cast<void>(handle_command(backend, std::move(request_ref), out));
	logger().debug("handle_commands after fork");
	return seastar::now();
      });
    });
}

seastar::future<> NBDHandler::run()
{
  logger().debug("About to listen on {}", uds_path);
  return seastar::do_with(
    seastar::engine().listen(
      seastar::socket_address{
	seastar::unix_domain_addr{uds_path}}),
    [=](auto &socket) {
      return seastar::keep_doing(
	[this, &socket] {
	  return socket.accept().then([this](auto acc) {
	    logger().debug("Accepted");
	    return seastar::do_with(
	      std::move(acc.connection),
	      [this](auto &conn) {
		return seastar::do_with(
		  conn.input(),
		  RequestWriter{conn.output()},
		  [&, this](auto &input, auto &output) {
		    return send_negotiation(
		      backend.get_size(),
		      output.stream
		    ).then([&, this] {
		      return handle_commands(backend, input, output);
		    }).finally([&] {
		      return input.close();
		    }).finally([&] {
		      return output.close();
		    }).handle_exception([](auto e) {
		      return seastar::now();
		    });
		  });
	      });
	  });
	});
    });
}

class TMDriver final : public BlockDriver {
  const config_t config;
  std::unique_ptr<segment_manager::block::BlockSegmentManager> segment_manager;
  std::unique_ptr<TransactionManager> tm;

public:
  TMDriver(config_t config) : config(config) {}
  ~TMDriver() final {}

  bufferptr get_buffer(size_t size) final {
    return ceph::buffer::create_page_aligned(size);
  }

  seastar::future<> write(
    off_t offset,
    bufferptr ptr) final {
    logger().debug("Writing offset {}", offset);
    assert(offset % segment_manager->get_block_size() == 0);
    assert((ptr.length() % (size_t)segment_manager->get_block_size()) == 0);
    return repeat_eagain([this, offset, ptr=std::move(ptr)] {
      return seastar::do_with(
	tm->create_transaction(),
	ptr,
	[this, offset](auto &t, auto &ptr) mutable {
	  return tm->dec_ref(
	    *t,
	    offset
	  ).safe_then([](auto){}).handle_error(
	    crimson::ct_error::enoent::handle([](auto) { return seastar::now(); }),
	    crimson::ct_error::pass_further_all{}
	  ).safe_then([=, &t, &ptr] {
	    logger().debug("dec_ref complete");
	    return tm->alloc_extent<TestBlock>(
	      *t,
	      offset,
	      ptr.length());
	  }).safe_then([=, &t, &ptr](auto ext) mutable {
	    assert(ext->get_laddr() == (size_t)offset);
	    assert(ext->get_bptr().length() == ptr.length());
	    ext->get_bptr().swap(ptr);
	    logger().debug("submitting transaction");
	    return tm->submit_transaction(std::move(t));
	  });
	});
    }).handle_error(
      crimson::ct_error::assert_all{"store-nbd write"}
    );
  }

  seastar::future<bufferlist> read(
    off_t offset,
    size_t size) final {
    logger().debug("Reading offset {}", offset);
    assert(offset % segment_manager->get_block_size() == 0);
    assert(size % (size_t)segment_manager->get_block_size() == 0);
    auto blptrret = std::make_unique<bufferlist>();
    auto &blret = *blptrret;
    return repeat_eagain([=, &blret] {
      return seastar::do_with(
	tm->create_transaction(),
	[=, &blret](auto &t) {
	  return tm->read_extents<TestBlock>(*t, offset, size
	  ).safe_then([=, &blret](auto ext_list) mutable {
	    size_t cur = offset;
	    for (auto &i: ext_list) {
	      if (cur != i.first) {
		assert(cur < i.first);
		blret.append_zero(i.first - cur);
		cur = i.first;
	      }
	      blret.append(i.second->get_bptr());
	      cur += i.second->get_bptr().length();
	    }
	    if (blret.length() != size) {
	      assert(blret.length() < size);
	      blret.append_zero(size - blret.length());
	    }
	  });
	});
    }).handle_error(
      crimson::ct_error::assert_all{"store-nbd read"}
    ).then([blptrret=std::move(blptrret)]() mutable {
      logger().debug("read complete");
      return std::move(*blptrret);
    });
  }

  void init() {
    auto segment_cleaner = std::make_unique<SegmentCleaner>(
      SegmentCleaner::config_t::default_from_segment_manager(
	*segment_manager),
      true);
    auto journal = std::make_unique<Journal>(*segment_manager);
    auto cache = std::make_unique<Cache>(*segment_manager);
    auto lba_manager = lba_manager::create_lba_manager(*segment_manager, *cache);

    journal->set_segment_provider(&*segment_cleaner);

    tm = std::make_unique<TransactionManager>(
      *segment_manager,
      std::move(segment_cleaner),
      std::move(journal),
      std::move(cache),
      std::move(lba_manager));
  }

  void clear() {
    tm.reset();
  }

  size_t get_size() const final {
    return segment_manager->get_size() * .5;
  }

  seastar::future<> mkfs() {
    assert(config.path);
    segment_manager = std::make_unique<
      segment_manager::block::BlockSegmentManager
      >();
    logger().debug("mkfs");
    BlockSegmentManager::mkfs_config_t block_config{
      *config.path, config.segment_size, config.total_device_size
    };
    block_config.meta.seastore_id.generate_random();
    return segment_manager->mkfs(std::move(block_config)
    ).safe_then([this] {
      logger().debug("");
      return segment_manager->mount({ *config.path });
    }).safe_then([this] {
      init();
      logger().debug("tm mkfs");
      return tm->mkfs();
    }).safe_then([this] {
      logger().debug("tm close");
      return tm->close();
    }).safe_then([this] {
      logger().debug("sm close");
      return segment_manager->close();
    }).safe_then([this] {
      clear();
      logger().debug("mkfs complete");
      return TransactionManager::mkfs_ertr::now();
    }).handle_error(
      crimson::ct_error::assert_all{
	"Invalid errror during TMDriver::mkfs"
      }
    );
  }

  seastar::future<> mount() final {
    return (config.mkfs ? mkfs() : seastar::now()
    ).then([this] {
      segment_manager = std::make_unique<
	segment_manager::block::BlockSegmentManager
	>();
      return segment_manager->mount({ *config.path });
    }).safe_then([this] {
      init();
      return tm->mount();
    }).handle_error(
      crimson::ct_error::assert_all{
	"Invalid errror during TMDriver::mount"
      }
    );
  };

  seastar::future<> close() final {
    return segment_manager->close(
    ).safe_then([this] {
      return tm->close();
    }).safe_then([this] {
      clear();
      return seastar::now();
    }).handle_error(
      crimson::ct_error::assert_all{
	"Invalid errror during TMDriver::close"
      }
    );
  }
};

BlockDriverRef get_backend(BlockDriver::config_t config)
{
  if (config.type == "transaction_manager") {
    return std::make_unique<TMDriver>(config);
  } else {
    ceph_assert(0 == "invalid option");
    return BlockDriverRef();
  }
}
