#pragma once

#include "ObjectModel.h"

/* Overview
 *
 * class RadosIo
 *   An IoExerciser. A simple RADOS client that generates I/Os
 *   from IoOps. Uses an ObjectModel to track the data stored
 *   in the object. Uses DataBuffer to create and validate
 *   data buffers. When there are not barrier I/Os this may
 *   issue multiple async I/Os in parallel.
 * 
 */

namespace ceph {
  namespace io_exerciser {
    namespace data_generation {
      class DataGenerator;
    }
    
    class RadosIo: public Model {
    protected:
      librados::Rados& rados;
      boost::asio::io_context& asio;
      std::unique_ptr<ObjectModel> om;
      std::unique_ptr<ceph::io_exerciser::data_generation::DataGenerator> db;
      std::string pool;
      int threads;
      ceph::mutex& lock;
      ceph::condition_variable& cond;
      librados::IoCtx io;
      int outstanding_io;

      void start_io();
      void finish_io();
      void wait_for_io(int count);
      
    public:
      RadosIo(librados::Rados& rados,
              boost::asio::io_context& asio,
              const std::string& pool,
              const std::string& oid,
              uint64_t block_size,
              int seed,
              int threads,
              ceph::mutex& lock,
              ceph::condition_variable& cond);

      ~RadosIo();

      void allow_ec_overwrites(bool allow);

      class AsyncOpInfo {
      public:
        librados::ObjectReadOperation rop;
        librados::ObjectWriteOperation wop;
        ceph::buffer::list bl1;
        ceph::buffer::list bl2;
        ceph::buffer::list bl3;
        uint64_t offset1;
        uint64_t length1;
        uint64_t offset2;
        uint64_t length2;
        uint64_t offset3;
        uint64_t length3;

        AsyncOpInfo(uint64_t offset1 = 0, uint64_t length1 = 0,
                uint64_t offset2 = 0, uint64_t length2 = 0,
                uint64_t offset3 = 0, uint64_t length3 = 0 );
        ~AsyncOpInfo() = default;
      };

      // Must be called with lock held
      bool readyForIoOp(IoOp& op);
      
      void applyIoOp(IoOp& op);
    };
  }
}