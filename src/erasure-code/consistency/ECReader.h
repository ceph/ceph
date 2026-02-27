#pragma once

#include <boost/asio/io_context.hpp>
#include <boost/program_options.hpp>
#include "librados/librados_asio.h"
#include "global/global_init.h"
#include "global/global_context.h"

#define dout_context g_ceph_context

namespace ceph {
namespace consistency {
class ReadResult {
  private:
    std::string oid;
    boost::system::error_code ec;
    ceph::bufferlist data;

  public:
    std::string get_oid() const { return oid; }
    boost::system::error_code get_ec() const { return ec; }
    ceph::bufferlist get_data() const { return data; }
    ReadResult(std::string oid,
               boost::system::error_code ec,
               ceph::bufferlist data) :
      oid(oid),
      ec(ec),
      data(data) {}
};

class Read {
  private:
    std::string oid;
    uint64_t block_size;
    uint64_t offset;
    uint64_t length;

  public:
    Read(const std::string& oid,
         uint64_t block_size,
         uint64_t offset,
         uint64_t length);
    std::string get_oid(void);
    uint64_t get_block_size(void);
    uint64_t get_offset(void);
    uint64_t get_length(void);
};
class ECReader {
  private:
    librados::Rados& rados;
    boost::asio::io_context& asio;
    std::string pool_name;
    std::string oid;
    librados::IoCtx io;
    ceph::condition_variable cond;
    std::vector<ReadResult> results;
    ceph::mutex lock;
    int outstanding_io;

  public:
    ECReader(librados::Rados& rados,
             boost::asio::io_context& asio,
             const std::string& pool);
    uint64_t get_object_size(std::string oid);
    void do_read(Read read);
    void start_io(void);
    void finish_io(void);
    void wait_for_io(void);
    std::vector<ReadResult>* get_results(void);
    void clear_results(void);
};
}
}