#pragma once

#include <memory>
#include <random>

#include "ObjectModel.h"
#include "include/buffer.h"

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
  static std::unique_ptr<DataGenerator> create_generator(
      GenerationType generatorType, const ObjectModel& model);
  virtual bufferlist generate_data(uint64_t length, uint64_t offset) = 0;
  virtual bool validate(bufferlist& bufferlist, uint64_t offset,
                        uint64_t length);

  // Used for testing debug outputs from data generation
  virtual bufferlist generate_wrong_data(uint64_t offset, uint64_t length);

  using SeedBytes = int;

 protected:
  const ObjectModel& m_model;

  DataGenerator(const ObjectModel& model) : m_model(model) {}
};

class SeededRandomGenerator : public DataGenerator {
 public:
  SeededRandomGenerator(const ObjectModel& model) : DataGenerator(model) {}

  virtual bufferptr generate_block(uint64_t offset);
  bufferlist generate_data(uint64_t length, uint64_t offset) override;
  virtual bufferptr generate_wrong_block(uint64_t offset);
  bufferlist generate_wrong_data(uint64_t offset,
                                         uint64_t length) override;
};

class HeaderedSeededRandomGenerator : public SeededRandomGenerator {
 public:
  HeaderedSeededRandomGenerator(
      const ObjectModel& model,
      std::optional<uint64_t> unique_run_id = std::nullopt);

  bufferptr generate_block(uint64_t offset) override;
  bufferptr generate_wrong_block(uint64_t offset) override;
  bool validate(bufferlist& bufferlist, uint64_t offset,
                uint64_t length) override;

 private:
  using UniqueIdBytes = uint64_t;
  using SeedBytes = int;
  using TimeBytes = uint64_t;

  enum class ErrorType {
    RUN_ID_MISMATCH,
    SEED_MISMATCH,
    DATA_MISMATCH,
    DATA_NOT_FOUND,
    UNKNOWN
  };

  constexpr uint8_t headerStart() const { return 0; };
  constexpr uint8_t uniqueIdStart() const { return headerStart(); };
  constexpr uint8_t uniqueIdLength() const { return sizeof(UniqueIdBytes); };
  constexpr uint8_t seedStart() const {
    return uniqueIdStart() + uniqueIdLength();
  };
  constexpr uint8_t seedLength() const { return sizeof(SeedBytes); };
  constexpr uint8_t timeStart() const { return seedStart() + seedLength(); };
  constexpr uint8_t timeLength() const { return sizeof(TimeBytes); };
  constexpr uint8_t timeEnd() const { return timeStart() + timeLength(); };
  constexpr uint8_t headerLength() const {
    return uniqueIdLength() + seedLength() + timeLength();
  };
  constexpr uint8_t bodyStart() const {
    return headerStart() + headerLength();
  };

  const UniqueIdBytes readUniqueRunId(uint64_t block_offset,
                                      const bufferlist& bufferlist);
  const SeedBytes readSeed(uint64_t block_offset, const bufferlist& bufferlist);
  const TimeBytes readDateTime(uint64_t block_offset,
                               const bufferlist& bufferlist);

  const UniqueIdBytes unique_run_id;

  uint64_t generate_unique_run_id();

  bool validate_block(uint64_t block_offset, const char* buffer_start);

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

  void printDebugInformationForRunIdMismatchRange(
      uint64_t read_offset, uint64_t start_block_offset,
      uint64_t range_length_in_blocks, const bufferlist& bufferlist);
  void printDebugInformationForSeedMismatchRange(
      uint64_t read_offset, uint64_t start_block_offset,
      uint64_t range_length_in_blocks, const bufferlist& bufferlist);
  void printDebugInformationDataBodyMismatchRange(
      uint64_t read_offset, uint64_t start_block_offset,
      uint64_t range_length_in_blocks, const bufferlist& bufferlist);
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
}  // namespace data_generation
}  // namespace io_exerciser
}  // namespace ceph
