#include "ECReader.h"

#include <boost/program_options.hpp>


using ECReader = ceph::consistency::ECReader;
using ReadResult = ceph::consistency::ReadResult;
using Read = ceph::consistency::Read;

Read::Read(const std::string& oid,
           uint64_t block_size,
           uint64_t offset,
           uint64_t length) :
oid(oid),
block_size(block_size),
offset(offset),
length(length) {}

std::string Read::get_oid() { return oid; }
uint64_t Read::get_block_size()  { return block_size; }
uint64_t Read::get_offset() { return offset; }
uint64_t Read::get_length() { return length; }


ECReader::ECReader(librados::Rados& rados,
                   boost::asio::io_context& asio,
                   const std::string& pool_name) :
  rados(rados),
  asio(asio),
  pool_name(pool_name),
  lock(ceph::make_mutex("ECReader::lock")),
  outstanding_io(0)
{
  int rc;
  rc = rados.ioctx_create(pool_name.c_str(), io);
  ceph_assert(rc == 0);
}

void ECReader::start_io()
{
  std::lock_guard l(lock);
  outstanding_io++;
}

void ECReader::finish_io()
{
  std::lock_guard l(lock);
  ceph_assert(outstanding_io > 0);
  outstanding_io--;
  cond.notify_all();
}

void ECReader::wait_for_io()
{
  std::unique_lock l(lock);
  while (outstanding_io > 0) {
    cond.wait(l);
  }
}

/**
 * Send a syncronous stat request to librados.
 *
 * @param oid Object ID.
 * @returns The size of the object, -1 if the stat failed.
 */
uint64_t ECReader::get_object_size(std::string oid)
{
  uint64_t size;
  int r = io.stat(oid, &size, nullptr);
  if (r != 0) {
    return -1;
  }
  return size;
}

/**
 * Send an async read request to librados. Push the returned data
 * and oid into the results vector. 
 *
 * @param read Read object containing oid, length, offset and block size
 */
void ECReader::do_read(Read read)
{
  start_io();
  librados::ObjectReadOperation op;
  op.read(read.get_offset() * read.get_block_size(),
          read.get_length() * read.get_block_size(),
          nullptr, nullptr);

  std::string oid = read.get_oid();
  auto read_cb = [&, oid](boost::system::error_code ec,
                         version_t ver,
                         bufferlist outbl) {
    results.push_back({oid, ec, outbl});
    finish_io();
  };

  librados::async_operate(asio.get_executor(), io, read.get_oid(),
                          std::move(op), 0, nullptr, read_cb);
}

/**
 * Wait for all outstanding reads to complete then return the results vector.
 * @returns Vector containing the results of all reads
 */
std::vector<ReadResult>* ECReader::get_results()
{
  wait_for_io();
  return &results;
}

/**
 * Clear the results vector.
 */
void ECReader::clear_results()
{
  results.clear();
}