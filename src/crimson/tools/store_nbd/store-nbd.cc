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

#include "crimson/common/log.h"
#include "crimson/common/config_proxy.h"

#include "test/crimson/seastar_runner.h"

#include "block_driver.h"

namespace po = boost::program_options;

using namespace ceph;

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}

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
  std::optional<seastar::promise<>> wait_pending;
  int pending = 0;

  RequestWriter(
    seastar::output_stream<char> &&stream) : stream(std::move(stream)) {}
  RequestWriter(RequestWriter &&) = default;

  void inc_pending() {
    ++pending;
  }

  void dec_pending() {
    ceph_assert(pending > 0);
    --pending;
    if (pending == 0 && wait_pending) {
      (*wait_pending).set_value();
    }
  }

  seastar::future<> complete(request_context_t::ref &&req) {
    auto &request = *req;
    return lock.write_lock(
    ).then([&request, this] {
      return request.write_reply(stream);
    }).finally([&, this, req=std::move(req)] {
      lock.write_unlock();
      logger().debug("complete");
      return seastar::now();
    });
  }

  seastar::future<> close() {
    ceph_assert(!wait_pending);
    auto do_wait_pending = seastar::now();
    if (pending > 0) {
      wait_pending = seastar::promise<>();
      do_wait_pending = (*wait_pending).get_future();
    }
    return do_wait_pending.then([this] {
      ceph_assert(pending == 0);
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
    return crimson::common::sharded_conf(
    ).start(EntityName{}, string_view{"ceph"}
    ).then([=] {
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
    }).then([=] {
      return crimson::common::sharded_conf().stop();
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
	out.inc_pending();
	static_cast<void>(
	  handle_command(
	    backend, std::move(request_ref), out
	  ).finally([&out] {
	    out.dec_pending();
	  })
	);
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
		      logger().error("NBDHandler::run saw exception {}", e);
		      return seastar::now();
		    });
		  });
	      });
	  });
	});
    });
}
