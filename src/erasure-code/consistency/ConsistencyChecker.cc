#include "ConsistencyChecker.h"

#include "RadosCommands.h"
#include "Pool.h"
#include "ECReader.h"
#include "ECEncoder.h"
#include "ECEncoderSwitch.h"

using ConsistencyChecker = ceph::consistency::ConsistencyChecker;

using Read = ceph::consistency::Read;
using ReadResult = ceph::consistency::ReadResult;
using bufferlist = ceph::bufferlist;

ConsistencyChecker::ConsistencyChecker(librados::Rados &rados,
                                       boost::asio::io_context& asio,
                                       const std::string& pool_name,
                                       int stripe_unit) :
  rados(rados),
  asio(asio),
  reader(ceph::consistency::ECReader(rados, asio, pool_name)),
  commands(ceph::consistency::RadosCommands(rados)),
  pool(pool_name,
       commands.get_ec_profile_for_pool(pool_name),
       commands.get_pool_allow_ec_optimizations(pool_name)),
  encoder(ceph::consistency::ECEncoderSwitch(pool.get_ec_profile(),
                                             stripe_unit,
                                             commands.get_pool_allow_ec_optimizations(pool_name)
                                            )) {}

/**
 * Perform an end-to-end read and consistency check on a single object.
 * Current implementation only supports reading the entire object, so length and
 * offset should normally be 0.
 *
 * @param oid string Name of the pool to perform inject on
 * @param block_size int Block size for the data being read
 * @param offset int Which offset to read from
 * @param length int How much data of each shard to read
 * @return bool true if consistent, otherwise false
 */
bool ConsistencyChecker::single_read_and_check_consistency(const std::string& oid,
                                                           int block_size,
                                                           int offset,
                                                           int length)
{
  clear_results();
  std::string error_message = "";
  bool success = true;

  auto read = Read(oid, block_size, offset, length);
  queue_ec_read(read);

  auto read_results = reader.get_results();
  int result_count = read_results->size();
  if (result_count != 1) {
    error_message = "Incorrect number of RADOS read results returned, count: "
                    + std::to_string(result_count);
    success = false;
  }

  ReadResult read_result = (*read_results)[0];
  boost::system::error_code ec = read_result.get_ec();
  if (success && ec != boost::system::errc::success) {
    error_message = "RADOS Read failed, error message: " + ec.message();
    success = false;
  }

  if (success && read_result.get_data().length() == 0) {
    error_message = "Empty object returned from RADOS read.";
    success = false;
  }

  if (success && !check_object_consistency(oid, read_result.get_data())) {
    error_message = "Generated parity did not match read in parity shards.";
    success = false;
  }

  results.push_back({oid, error_message, success});
  commands.inject_clear_parity_read_on_primary_osd(pool.get_pool_name(),
                                                   oid);
  return success;
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
 * @param oid string The object ID of the object being checked
 * @param inbl bufferlist The entire contents of the object, including parities
 * @param stripe_unit int The chunk size for the object
 */
bool ConsistencyChecker::check_object_consistency(const std::string& oid,
                                                  const bufferlist& inbl)
{
  bool is_optimized = pool.has_optimizations_enabled();
  std::pair<bufferlist, bufferlist> data_and_parity;
  data_and_parity = split_data_and_parity(oid, inbl, encoder.get_k(), 
                                          encoder.get_m(), is_optimized);

  std::optional<bufferlist> outbl;
  outbl = encoder.do_encode(data_and_parity.first);

  if (!outbl.has_value()) {
    return false;
  }

  return buffers_match(outbl.value(), data_and_parity.second);
}

void ConsistencyChecker::print_results(std::ostream& out)
{
  out << "Results:" << std::endl;
  for (const auto &r : results) {
    std::string result_str = (r.get_result()) ? "Passed" : "Failed";
    std::string error_str = r.get_error_message();
    out << "Object ID " << r.get_oid() << ": " << result_str << std::endl;
    if (!error_str.empty()) {
      out << "Error Message: " << error_str << std::endl;
    }
  }

  int count = results.size();
  std::string obj_str = (count == 1) ? "object checked." : "objects checked.";
  out << "Total: " << count << " " << obj_str << std::endl;
}

std::pair<bufferlist, bufferlist>
  ConsistencyChecker::split_data_and_parity(const std::string& oid,
                                            const bufferlist& read,
                                            int k, int m,
                                            bool is_optimized)
{
  uint64_t data_size, parity_size;

  // Optimized EC parity read should return the exact object size + parity shards
  // Legacy EC parity read will return the entire padded data shards + parity shards
  data_size = is_optimized ? reader.get_object_size(oid) : (read.length() / (k + m)) * k;
  parity_size = read.length() - data_size;

  bufferlist data, parity;
  auto it = read.begin();
  it.copy(data_size, data);
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