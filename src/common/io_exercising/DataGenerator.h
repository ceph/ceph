// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <memory>
#include <random>

#include "include/buffer.h"
#include "Model.h"

/* Overview
 *
 * class DataGenerator
 *   Generates data buffers for write I/Os using state queried
 *   from ObjectModel. Validates data buffers for read I/Os
 *   against the state in the ObjectModel. If a data miscompare
 *   is detected provide debug information about the state of the
 *   object, the buffer that was read and the expected buffer.
 *
 */
namespace Ceph {
  namespace DataGenerator {
    enum class GenerationType {
      Zero,
      One,
      SeededRandom,
      HeaderedSeededRandom,
      ZeroParity,
      OneParity,
      ZeroPrimaryChunk,
      OnePrimaryChunk,
      ZeroSecondaryChunk,
      OneSecondaryChunk,
      Mixed  
    };
  
    class DataGenerator {
    public:
      static std::unique_ptr<Ceph::DataGenerator::DataGenerator> create_generator(GenerationType generatorType, const ObjectModel& model);

      virtual void generate_data(uint64_t length, uint64_t offset, ceph::bufferlist& retlist)=0;
      virtual bool validate(const ceph::bufferlist& bufferlist, uint64_t offset, uint64_t length);

      virtual ~DataGenerator() = default;

    protected:
      DataGenerator(const ObjectModel& model) : m_model(model) {}

      const ObjectModel& m_model;

protected:
      ceph::bufferlist m_validationBuffer;
    };

    // Interesting things to generate:
    // All zeros
    // All ones
    // All zero parities
    // All one parities
    // Random data
    // Flip-flopping parities completely bit-wise from their previous value

    // I can decode using the erasure code plugins encode_chunks and decode_chunks
    // functions to generate data from interesting parities
    class ZeroGenerator : public DataGenerator
    {
      public:
        void generate_data(uint64_t length, uint64_t offset, ceph::bufferlist& retlist) override;
    };

    class OneGenerator : public DataGenerator
    {
      public:
        void generate_data(uint64_t length, uint64_t offset, ceph::bufferlist& retlist) override;
    };

    class SeededRandomGenerator : public DataGenerator
    {
      public:
        SeededRandomGenerator(const ObjectModel& model) : DataGenerator(model) {}
        virtual ceph::bufferptr generate_block(uint64_t offset);
        virtual void generate_data(uint64_t length, uint64_t offset, ceph::bufferlist& retlist) override;
      
      protected:
      ceph::util::random_number_generator<char> random_generator = ceph::util::random_number_generator<char>();
    };

    class HeaderedSeededRandomGenerator : public SeededRandomGenerator
    {
      public:
        HeaderedSeededRandomGenerator(const ObjectModel& model) : SeededRandomGenerator(model) {}
        void generate_data(uint64_t length, uint64_t offset, ceph::bufferlist& retlist) override;
        bool validate(const ceph::bufferlist& bufferlist, uint64_t offset, uint64_t length) override;
    };

    class ZeroParityGenerator : public DataGenerator
    {
      public:
      void generate_data(uint64_t length, uint64_t offset, ceph::bufferlist& retlist) override;
    };

    class OneParityGenerator : public DataGenerator
    {
      public:
      void generate_data(uint64_t length, uint64_t offset, ceph::bufferlist& retlist) override;
    };

    class ZeroPrimaryChunkDataGenerator : public DataGenerator
    {
      public:
      void generate_data(uint64_t length, uint64_t offset, ceph::bufferlist& retlist) override;
    };

    class OnePrimaryChunkDataGenerator : public DataGenerator
    {
      public:
      void generate_data(uint64_t length, uint64_t offset, ceph::bufferlist& retlist) override;
    };

    class ZeroSecondaryChunkDataGenerator : public DataGenerator
    {
      public:
      void generate_data(uint64_t length, uint64_t offset, ceph::bufferlist& retlist) override;
    };

    class OneSecondaryChunkDataGenerator : public DataGenerator
    {
      public:
      void generate_data(uint64_t length, uint64_t offset, ceph::bufferlist& retlist) override;
    };

    class MultiGenerator : public DataGenerator
    {
      public:
      void generate_data(uint64_t length, uint64_t offset, ceph::bufferlist& retlist) override;
    };
  }
}
