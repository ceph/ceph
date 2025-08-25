#include "RadosCommands.h"
#include "common/ceph_json.h"
#include "common/json/OSDStructures.h"
#include "erasure-code/ErasureCodePlugin.h"
#include <boost/algorithm/string.hpp>

using RadosCommands = ceph::consistency::RadosCommands;

RadosCommands::RadosCommands(librados::Rados& rados) :
  rados(rados),
  formatter(new JSONFormatter(true))
{
}

/**
 * Return the index of the acting primary OSD for the given pool
 * and object name. Assert on failure.
 *
 * @param pool_name string Name of the pool to find acting primary of
 * @param oid string OID of the object to find acting primary of
 * @returns int ID of the acting primary OSD
 */
int RadosCommands::get_primary_osd(const std::string& pool_name,
                                   const std::string& oid)
{
  ceph::messaging::osd::OSDMapRequest osd_map_request{pool_name, oid, ""};
  encode_json("OSDMapRequest", osd_map_request, formatter.get());

  std::ostringstream oss;
  formatter.get()->flush(oss);

  ceph::bufferlist inbl, outbl;
  int rc = rados.mon_command(oss.str(), inbl, &outbl, nullptr);
  ceph_assert(rc == 0);

  JSONParser p;
  bool success = p.parse(outbl.c_str(), outbl.length());
  ceph_assert(success);

  ceph::messaging::osd::OSDMapReply reply;
  reply.decode_json(&p);
  int osd = reply.acting_primary;
  ceph_assert(osd >= 0);

  return osd;
}

/**
 * Send a mon command to fetch the value of the 'allow_ec_optimizations' flag for the
 * specified pool and return it.
 *
 * @param pool_name string Name of the pool to get the erasure code profile for
 * @returns bool Whether allow EC optimizations is set on the pool
 */
bool RadosCommands::get_pool_allow_ec_optimizations(const std::string& pool_name)
{
  ceph::messaging::osd::OSDPoolGetRequest osd_pool_get_request{pool_name, "allow_ec_optimizations"};
  encode_json("OSDPoolGetRequest", osd_pool_get_request, formatter.get());

  std::ostringstream oss;
  formatter.get()->flush(oss);

  ceph::bufferlist inbl, outbl;
  int rc = rados.mon_command(oss.str(), inbl, &outbl, nullptr);
  ceph_assert(rc == 0);

  JSONParser p;
  bool success = p.parse(outbl.c_str(), outbl.length());
  ceph_assert(success);

  ceph::messaging::osd::OSDPoolGetReply osd_pool_get_reply;
  osd_pool_get_reply.decode_json(&p);

  return osd_pool_get_reply.allow_ec_optimizations;
}

/**
 * Send a mon command to fetch the name of the erasure code profile for the
 * specified pool and return it.
 *
 * @param pool_name string Name of the pool to get the erasure code profile for
 * @returns string The erasure code profile for the specified pool
 */
std::string RadosCommands::get_pool_ec_profile_name(const std::string& pool_name)
{
  ceph::messaging::osd::OSDPoolGetRequest osd_pool_get_request{pool_name};
  encode_json("OSDPoolGetRequest", osd_pool_get_request, formatter.get());

  std::ostringstream oss;
  formatter.get()->flush(oss);

  ceph::bufferlist inbl, outbl;
  int rc = rados.mon_command(oss.str(), inbl, &outbl, nullptr);
  ceph_assert(rc == 0);

  JSONParser p;
  bool success = p.parse(outbl.c_str(), outbl.length());
  ceph_assert(success);

  ceph::messaging::osd::OSDPoolGetReply osd_pool_get_reply;
  osd_pool_get_reply.decode_json(&p);

  return osd_pool_get_reply.erasure_code_profile;
}

/**
 * Fetch the erasure code profile for the specified pool and return it.
 *
 * @param pool_name string Name of the pool to get the EC profile for
 * @returns ErasureCodeProfile The EC profile for the specified pool
 */
ceph::ErasureCodeProfile RadosCommands::get_ec_profile_for_pool(const std::string& pool_name)
{
  ceph::messaging::osd::OSDECProfileGetRequest osd_ec_profile_get_req{
      get_pool_ec_profile_name(pool_name), "plain"};
  encode_json("OSDECProfileGetRequest", osd_ec_profile_get_req, formatter.get());

  std::ostringstream oss;
  formatter.get()->flush(oss);

  ceph::bufferlist inbl, outbl;
  int rc = rados.mon_command(oss.str(), inbl, &outbl, nullptr);
  ceph_assert(rc == 0);

  // Parse the string output into an ErasureCodeProfile
  ceph::ErasureCodeProfile profile;
  std::string line, key, val, out(outbl.c_str(), outbl.length());
  std::stringstream ss(out);

  while (std::getline(ss, line)) {
    key = line.substr(0, line.find("="));
    val = line.substr(line.find("=") + 1, line.length() - 1);
    profile.emplace(key, val);
  }

  return profile;
}

/**
 * Get chunk size for pool with the supplied name
 *
 * @param pool_name string Name of the pool to get chunk size of
 * @return int the chunk size of the pool
 */
int RadosCommands::get_ec_chunk_size_for_pool(const std::string& pool_name)
{
  ceph::ErasureCodeProfile profile = get_ec_profile_for_pool(pool_name);
  return (profile.contains("stripe_unit") ? std::stol(profile["stripe_unit"]) : 4096);
}

/**
 * Inject the parity read inject on the acting primary
 * for the specified object and pool. Assert on failure.
 *
 * @param pool_name string Name of the pool to perform inject on
 * @param oid string OID of the object to perform inject on
 */
void RadosCommands::inject_parity_read_on_primary_osd(const std::string& pool_name,
                                                      const std::string& oid)
{
  int primary_osd = get_primary_osd(pool_name, oid);
  ceph::messaging::osd::InjectECParityRead parity_read_req{pool_name, oid};
  encode_json("InjectECParityRead", parity_read_req, formatter.get());

  std::ostringstream oss;
  formatter.get()->flush(oss);

  ceph::bufferlist inbl, outbl;
  int rc = rados.osd_command(primary_osd, oss.str(), inbl, &outbl, nullptr);
  ceph_assert(rc == 0);
}

/**
 * RadosCommands the clear parity read inject on the acting primary
 * for the specified object and pool. Assert on failure.
 *
 * @param pool_name string Name of the pool to perform inject on
 * @param oid string OID of the object to perform inject on
 */
void RadosCommands::inject_clear_parity_read_on_primary_osd(const std::string& pool_name,
                                                       const std::string& oid)
{
  int primary_osd = get_primary_osd(pool_name, oid);
  ceph::messaging::osd::InjectECClearParityRead clear_parity_read_req{pool_name, oid};
  encode_json("InjectECClearParityRead", clear_parity_read_req, formatter.get());

  std::ostringstream oss;
  formatter.get()->flush(oss);

  ceph::bufferlist inbl, outbl;
  int rc = rados.osd_command(primary_osd, oss.str(), inbl, &outbl, nullptr);
  ceph_assert(rc == 0);
}