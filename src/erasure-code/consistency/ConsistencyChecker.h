#pragma once

#include <boost/asio/io_context.hpp>
#include <boost/program_options.hpp>

#include "librados/librados_asio.h"

#include "global/global_init.h"
#include "global/global_context.h"

#include "Pool.h"
#include "ECReader.h"
#include "RadosCommands.h"
#include "ECEncoder.h"

#define dout_context g_ceph_context

namespace ceph {
  namespace consistency {
    typedef std::pair<std::string, bool> ConsistencyCheckResult;
    class ConsistencyChecker {
      protected:
        librados::Rados& rados;
        boost::asio::io_context& asio;
        ceph::consistency::ECReader reader;
        ceph::consistency::RadosCommands commands;
        ceph::consistency::Pool pool;
        std::vector<ConsistencyCheckResult> results;
        bool buffers_match(const bufferlist& b1, const bufferlist& b2);
        std::pair<bufferlist, bufferlist> split_data_and_parity(const bufferlist& read, 
                                                                ErasureCodeProfile profile);

      public:
        ConsistencyChecker(librados::Rados& rados,
                           boost::asio::io_context& asio,
                           const std::string& pool_name);
        void queue_ec_read(Read read);
        bool check_object_consistency(const bufferlist& inbl, int stripe_unit);
        void print_results(std::ostream& out);
        void clear_results();
        bool single_read_and_check_consistency(const std::string& oid,
                                               int block_size,
                                               int offset,
                                               int length,
                                               int stripe_unit);
    };
  }
}