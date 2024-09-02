#pragma once

#include <memory>
#include <random>

#include "include/buffer.h"
#include "ObjectModel.h"

/* Overview
 *
 * class DataGenerator
 *   Generates data buffers for write I/Os using state queried
 *   from ObjectModel. Validates data buffers for read I/Os
 *   against the state in the ObjectModel. If a data miscompare
 *   is detected provide debug information about the state of the
 *   object, the buffer that was read and the expected buffer.
 *
 *
 * class DataGenerator::DataGenerationSingleton
 *   A singleton object created on first use to create and store
 *   a unique run identifier. Can either be created supplying an id
 *   or it can be generated at first use. Id cannot and should not be
 *   changed after being created.
 *
 * 
 * class SeededRandomGenerator
 *   Inherits from DataGenerator. Generates entirely random patterns
 *   based on the seed retrieved by the model.
 * 
 * 
 * class HeaderedSeededRandomGenerator
 *   Inherits from SeededDataGenerator. Generates entirely random patterns
 *   based on the seed retrieved by the model, however also appends a 
 *   header to the start of each block. This generator also provides
 *   a range of verbose debug options to help disagnose a miscompare
 *   whenever it detects unexpected data.
 */
 
namespace ceph {
  namespace io_exerciser {
    namespace data_generation {
      enum class GenerationType {
        SeededRandom,
        HeaderedSeededRandom
        // CompressedGenerator
        // MixedGenerator
      };
    
      class DataGenerator {
      public:
        virtual ~DataGenerator() = default;
        static std::unique_ptr<DataGenerator>
          create_generator(GenerationType generatorType,
                           const ObjectModel& model);
        virtual void generate_data(uint64_t length, uint64_t offset,
                                   bufferlist& retlist)=0;
        virtual bool validate(bufferlist& bufferlist, uint64_t offset,
                              uint64_t length);

        // Used for testing debug outputs from data generation
        virtual void generate_wrong_data(uint64_t offset, uint64_t length,
                                         bufferlist& retlist);

      protected:
        const ObjectModel& m_model;

        DataGenerator(const ObjectModel& model) : m_model(model) {}

        class DataGenerationSingleton {
        private:
          DataGenerationSingleton();
          DataGenerationSingleton(uint64_t unique_id);

          bool m_created = false;
          uint64_t m_uniqueId = 0;

          static DataGenerationSingleton m_singletonInstance;
          static std::mutex m_mutex;

        public:
          static const DataGenerationSingleton&
            createSpecificInstance(uint64_t unique_id);
          static const DataGenerationSingleton& getInstance();
          static const uint64_t getUniqueId();
        };
      };

      class SeededRandomGenerator : public DataGenerator
      {
        public:
          SeededRandomGenerator(const ObjectModel& model)
            : DataGenerator(model) {}

          virtual bufferptr generate_block(uint64_t offset);
          virtual void generate_data(uint64_t length, uint64_t offset,
                                     bufferlist& retlist) override;
      };

      class HeaderedSeededRandomGenerator : public SeededRandomGenerator
      {
        public:           
          HeaderedSeededRandomGenerator(const ObjectModel& model)
            : SeededRandomGenerator(model) {}

          bufferptr generate_block(uint64_t offset) override;
          void generate_wrong_data(uint64_t offset, uint64_t length,
                                   bufferlist& retlist) override;
          bool validate(bufferlist& bufferlist, uint64_t offset,
                        uint64_t length) override;

        private:
          using UniqueIdBytes = uint64_t;
          using SeedBytes = int;
          using TimeBytes = uint64_t;

          constexpr uint8_t headerStart() const
            { return 0; };
          constexpr uint8_t uniqueIdStart() const
            { return headerStart(); };
          constexpr uint8_t uniqueIdLength() const
            { return sizeof(UniqueIdBytes); };
          constexpr uint8_t seedStart() const
            { return uniqueIdStart() + uniqueIdLength(); };
          constexpr uint8_t seedLength() const
            { return sizeof(SeedBytes); };
          constexpr uint8_t timeStart() const
            { return seedStart() + seedLength(); };
          constexpr uint8_t timeLength() const
            { return sizeof(TimeBytes); };
          constexpr uint8_t timeEnd() const
            { return timeStart() + timeLength(); };
          constexpr uint8_t headerLength() const
            { return uniqueIdLength() + seedLength() + timeLength(); };
          constexpr uint8_t bodyStart() const
            { return headerStart() + headerLength(); };

          const UniqueIdBytes readUniqueRunId(uint64_t block_offset,
                                              const bufferlist& bufferlist);
          const SeedBytes readSeed(uint64_t block_offset,
                                   const bufferlist& bufferlist);
          const TimeBytes readDateTime(uint64_t block_offset,
                                       const bufferlist& bufferlist);

          bool validate_block(uint64_t block_offset, const char* buffer_start);

          enum class ErrorType {
            RUN_ID_MISMATCH,
            SEED_MISMATCH,
            DATA_MISMATCH,
            DATA_NOT_FOUND,
            UNKNOWN
          };

          const ErrorType getErrorTypeForBlock(uint64_t read_offset,
                                               uint64_t block_offset,
                                               const bufferlist& bufferlist);
          
          void printDebugInformationForBlock(uint64_t read_offset,
                                             uint64_t block_offset,
                                             const bufferlist& bufferlist);
          void printDebugInformationForRange(uint64_t read_offset,
                                             uint64_t start_block_offset,
                                             uint64_t range_length_in_blocks,
                                             ErrorType rangeError,
                                             const bufferlist& bufferlist);

          void printDebugInformationForRunIdMismatchRange(uint64_t read_offset,
                                                          uint64_t start_block_offset,
                                                          uint64_t range_length_in_blocks,
                                                          const bufferlist& bufferlist);
          void printDebugInformationForSeedMismatchRange(uint64_t read_offset,
                                                         uint64_t start_block_offset,
                                                         uint64_t range_length_in_blocks,
                                                         const bufferlist& bufferlist);
          void printDebugInformationDataBodyMismatchRange(uint64_t read_offset,
                                                          uint64_t start_block_offset,
                                                          uint64_t range_length_in_blocks,
                                                          const bufferlist& bufferlist);
          void printDebugInformationDataNotFoundRange(uint64_t ßread_offset,
                                                      uint64_t start_block_offset,
                                                      uint64_t range_length_in_blocks,
                                                      const bufferlist& bufferlist);
          void printDebugInformationCorruptRange(uint64_t read_offset,
                                                 uint64_t start_block_offset,
                                                 uint64_t range_length_in_blocks,
                                                 const bufferlist& bufferlist);

          void printDebugInformationForOffsets(uint64_t read_offset,
                                               std::vector<uint64_t> offsets,
                                               const bufferlist& bufferlist);
      };
    }
  }
}
