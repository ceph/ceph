#include "ConsistencyChecker.h"

#include "RadosCommands.h"
#include "Pool.h"
#include "ECReader.h"
#include "ECEncoder.h"

using ConsistencyChecker = ceph::consistency::ConsistencyChecker;

using Read = ceph::consistency::Read;
using ReadResult = ceph::consistency::ReadResult;
using bufferlist = ceph::bufferlist;


ConsistencyChecker::ConsistencyChecker(librados::Rados &rados,
                                       boost::asio::io_context& asio,
                                       const std::string& pool_name) :
  rados(rados),
  asio(asio),
  reader(ceph::consistency::ECReader(rados, asio, pool_name)),
  commands(ceph::consistency::RadosCommands(rados)),
  pool(pool_name, commands.get_ec_profile_for_pool(pool_name))
{
}

/**
 * Perform an end-to-end read and consistency check on a single object.
 * Current implementation only supports reading the entire object, so length and
 * offset should normally be 0.
 *
 * @param oid string Name of the pool to perform inject on
 * @param block_size int Block size for the data being read
 * @param offset int Which offset to read from
 * @param length int How much data of each shard to read
 * @param stripe_unit int Size of each chunk of the object
 * @return bool true if consistent, otherwise false
 */
bool ConsistencyChecker::single_read_and_check_consistency(const std::string& oid,
                                                           int block_size,
                                                           int offset,
                                                           int length,
                                                           int stripe_unit)
{
  clear_results();
  auto read = Read(oid, block_size, offset, length);
  queue_ec_read(read);

  auto read_results = reader.get_results();
  ceph_assert(read_results->size() == 1);

  ReadResult res = (*read_results)[0];

  bool is_consistent = check_object_consistency(res.second, stripe_unit);
  results.push_back(ConsistencyCheckResult(oid, is_consistent));
  commands.inject_clear_parity_read_on_primary_osd(pool.get_pool_name(),
                                                   oid);
  return is_consistent;
}

/**
 * Queue up an EC read with the parity read inject set
 *
 * @param read Object containing information about the read
 */
void ConsistencyChecker::queue_ec_read(Read read)
{
  commands.inject_parity_read_on_primary_osd(pool.get_pool_name(),
                                             read.get_oid());
  reader.do_read(read);
}

/**
 * Generate parities from the data and compare to the parity shards
 *
 * @param inbl bufferlist The entire contents of the object, including parities
 * @param stripe_unit int The chunk size for the object
 */
bool ConsistencyChecker::check_object_consistency(const bufferlist& inbl, int stripe_unit)
{
  std::pair<bufferlist, bufferlist> data_and_parity;
  data_and_parity = split_data_and_parity(inbl, pool.get_ec_profile());

  bufferlist outbl;
  auto encoder = ceph::consistency::ECEncoder(pool.get_ec_profile(), stripe_unit);
  encoder.do_encode(data_and_parity.first, outbl);

  return buffers_match(outbl, data_and_parity.second);
}

void ConsistencyChecker::print_results(std::ostream& out)
{
  out << "Results:" << std::endl;
  for (auto r : results) {
    std::string result_str = (r.second) ? "Passed" : "Failed";
    out << "Object ID " << r.first << ": " << result_str << std::endl;
  }
  int count = results.size();
  out << "Total: " << count << " objects checked." << std::endl;
}

std::pair<bufferlist, bufferlist>
  ConsistencyChecker::split_data_and_parity(const bufferlist& read,
                                            ErasureCodeProfile profile)
{
  uint8_t k = atoi(profile["k"].c_str());
  uint8_t m = atoi(profile["m"].c_str());
  uint64_t parity_index = (read.length() / (k + m)) * k;
  uint64_t parity_size = read.length() - parity_index;

  bufferlist data, parity;
  auto it = read.begin();
  it.copy(read.length() - parity_size, data);
  it.copy(parity_size, parity);
  return std::pair<bufferlist, bufferlist>(data, parity);
}

bool ConsistencyChecker::buffers_match(const bufferlist& b1,
                                       const bufferlist& b2)
{
  return (b1.contents_equal(b2));
}

void ConsistencyChecker::clear_results()
{
  reader.clear_results();
  results.clear();
}