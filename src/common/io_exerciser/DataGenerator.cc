// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "DataGenerator.h"

#include <chrono>
#include <iostream>
#include <stdexcept>

#include "ObjectModel.h"
#include "common/debug.h"
#include "common/dout.h"
#include "fmt/format.h"
#include "fmt/ranges.h"

#define dout_subsys ceph_subsys_rados
#define dout_context g_ceph_context

using DataGenerator = ceph::io_exerciser::data_generation::DataGenerator;
using SeededRandomGenerator =
    ceph::io_exerciser::data_generation ::SeededRandomGenerator;
using HeaderedSeededRandomGenerator =
    ceph::io_exerciser::data_generation ::HeaderedSeededRandomGenerator;

std::unique_ptr<DataGenerator> DataGenerator::create_generator(
    GenerationType generationType, const ObjectModel& model) {
  switch (generationType) {
    case GenerationType::SeededRandom:
      return std::make_unique<SeededRandomGenerator>(model);
    case GenerationType::HeaderedSeededRandom:
      return std::make_unique<HeaderedSeededRandomGenerator>(model);
    default:
      throw std::invalid_argument("Not yet implemented");
  }

  return nullptr;
}

bufferlist DataGenerator::generate_wrong_data(uint64_t offset,
                                              uint64_t length) {
  bufferlist retlist;
  uint64_t block_size = m_model.get_block_size();
  char buffer[block_size];
  for (uint64_t block_offset = offset; block_offset < offset + length;
       block_offset++) {
    std::memset(buffer, 0, block_size);
    retlist.append(ceph::bufferptr(buffer, block_size));
  }
  return retlist;
}

bool DataGenerator::validate(bufferlist& bufferlist, uint64_t offset,
                             uint64_t length) {
  return bufferlist.contents_equal(generate_data(offset, length));
}

ceph::bufferptr SeededRandomGenerator::generate_block(uint64_t block_offset) {
  uint64_t block_size = m_model.get_block_size();
  char buffer[block_size];
  SeedBytes seed = m_model.get_seed(block_offset);
  if (seed != 0) {
    std::mt19937_64 random_generator(seed);
    uint64_t rand1 = random_generator();
    uint64_t rand2 = random_generator();

    constexpr size_t generation_length = sizeof(uint64_t);

    for (uint64_t i = 0; i < block_size;
         i += (2 * generation_length), rand1++, rand2--) {
      std::memcpy(buffer + i, &rand1, generation_length);
      std::memcpy(buffer + i + generation_length, &rand2, generation_length);
    }

    size_t remainingBytes = block_size % (generation_length * 2);
    if (remainingBytes > generation_length) {
      size_t remainingBytes2 = remainingBytes - generation_length;
      std::memcpy(buffer + block_size - remainingBytes, &rand1, remainingBytes);
      std::memcpy(buffer + block_size - remainingBytes2, &rand2,
                  remainingBytes2);
    } else if (remainingBytes > 0) {
      std::memcpy(buffer + block_size - remainingBytes, &rand1, remainingBytes);
    }
  } else {
    std::memset(buffer, 0, block_size);
  }
  return ceph::bufferptr(buffer, block_size);
}

ceph::bufferptr SeededRandomGenerator::generate_wrong_block(
    uint64_t block_offset) {
  uint64_t block_size = m_model.get_block_size();
  char buffer[block_size];

  std::mt19937_64 random_generator(m_model.get_seed(block_offset));
  uint64_t rand1 = random_generator() - 1;
  uint64_t rand2 = random_generator() + 1;

  constexpr size_t generation_length = sizeof(uint64_t);

  for (uint64_t i = 0; i < block_size;
       i += (2 * generation_length), rand1++, rand2--) {
    std::memcpy(buffer + i, &rand1, generation_length);
    std::memcpy(buffer + i + generation_length, &rand2, generation_length);
  }

  size_t remainingBytes = block_size % (generation_length * 2);
  if (remainingBytes > generation_length) {
    size_t remainingBytes2 = remainingBytes - generation_length;
    std::memcpy(buffer + block_size - remainingBytes, &rand1, remainingBytes);
    std::memcpy(buffer + block_size - remainingBytes2, &rand2, remainingBytes2);
  } else if (remainingBytes > 0) {
    std::memcpy(buffer + block_size - remainingBytes, &rand1, remainingBytes);
  }

  return ceph::bufferptr(buffer, block_size);
}

bufferlist SeededRandomGenerator::generate_data(uint64_t offset,
                                                uint64_t length) {
  bufferlist retlist;

  for (uint64_t block_offset = offset; block_offset < offset + length;
       block_offset++) {
    retlist.append(generate_block(block_offset));
  }

  return retlist;
}

bufferlist SeededRandomGenerator::generate_wrong_data(uint64_t offset,
                                                      uint64_t length) {
  bufferlist retlist;

  for (uint64_t block_offset = offset; block_offset < offset + length;
       block_offset++) {
    retlist.append(generate_wrong_block(block_offset));
  }

  return retlist;
}

HeaderedSeededRandomGenerator ::HeaderedSeededRandomGenerator(
    const ObjectModel& model, std::optional<uint64_t> unique_run_id)
    : SeededRandomGenerator(model),
      unique_run_id(unique_run_id.value_or(generate_unique_run_id())) {}

uint64_t HeaderedSeededRandomGenerator::generate_unique_run_id() {
  std::mt19937_64 random_generator =
      std::mt19937_64(duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count());

  return random_generator();
}

ceph::bufferptr HeaderedSeededRandomGenerator::generate_block(
    uint64_t block_offset) {
  SeedBytes seed = m_model.get_seed(block_offset);
  TimeBytes current_time =
      duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count();

  ceph::bufferptr bufferptr =
      SeededRandomGenerator::generate_block(block_offset);

  if (seed != 0) {
    std::memcpy(bufferptr.c_str() + uniqueIdStart(), &unique_run_id,
                uniqueIdLength());
    std::memcpy(bufferptr.c_str() + seedStart(), &seed, seedLength());
    std::memcpy(bufferptr.c_str() + timeStart(), &current_time, timeLength());
  }

  return bufferptr;
}

ceph::bufferptr HeaderedSeededRandomGenerator::generate_wrong_block(
    uint64_t block_offset) {
  return HeaderedSeededRandomGenerator::generate_block(block_offset % 8);
}

const HeaderedSeededRandomGenerator::UniqueIdBytes
HeaderedSeededRandomGenerator::readUniqueRunId(uint64_t block_offset,
                                               const bufferlist& bufferlist) {
  UniqueIdBytes read_unique_run_id = 0;
  std::memcpy(
      &read_unique_run_id,
      &bufferlist[(block_offset * m_model.get_block_size()) + uniqueIdStart()],
      uniqueIdLength());
  return read_unique_run_id;
}

const HeaderedSeededRandomGenerator::SeedBytes
HeaderedSeededRandomGenerator::readSeed(uint64_t block_offset,
                                        const bufferlist& bufferlist) {
  SeedBytes read_seed = 0;
  std::memcpy(
      &read_seed,
      &bufferlist[(block_offset * m_model.get_block_size()) + seedStart()],
      seedLength());
  return read_seed;
}

const HeaderedSeededRandomGenerator::TimeBytes
HeaderedSeededRandomGenerator::readDateTime(uint64_t block_offset,
                                            const bufferlist& bufferlist) {
  TimeBytes read_time = 0;
  std::memcpy(
      &read_time,
      &bufferlist[(block_offset * m_model.get_block_size()) + timeStart()],
      timeLength());
  return read_time;
}

bool HeaderedSeededRandomGenerator::validate(bufferlist& bufferlist,
                                             uint64_t offset, uint64_t length) {
  std::vector<uint64_t> invalid_block_offsets;

  for (uint64_t block_offset = offset; block_offset < offset + length;
       block_offset++) {
    bool valid_block = validate_block(
        block_offset, (bufferlist.c_str() +
                       ((block_offset - offset) * m_model.get_block_size())));
    if (!valid_block) {
      invalid_block_offsets.push_back(block_offset);
    }
  }

  if (!invalid_block_offsets.empty()) {
    dout(0) << "Miscompare for read of " << m_model.get_oid() <<
      " offset=" << offset << " length=" << length << dendl;
    printDebugInformationForOffsets(offset, invalid_block_offsets, bufferlist);
  }

  return invalid_block_offsets.empty();
}

bool HeaderedSeededRandomGenerator::validate_block(uint64_t block_offset,
                                                   const char* buffer_start) {
  // We validate the block matches what we generate byte for byte
  // however we ignore the time section of the header
  ceph::bufferptr bufferptr = generate_block(block_offset);
  bool valid = strncmp(bufferptr.c_str(), buffer_start, timeStart()) == 0;
  valid = valid
              ? strncmp(bufferptr.c_str() + timeEnd(), buffer_start + timeEnd(),
                        m_model.get_block_size() - timeEnd()) == 0
              : valid;
  return valid;
}

const HeaderedSeededRandomGenerator::ErrorType
HeaderedSeededRandomGenerator::getErrorTypeForBlock(
    uint64_t read_offset, uint64_t block_offset, const bufferlist& bufferlist) {
  try {
    UniqueIdBytes read_unique_run_id =
        readUniqueRunId(block_offset - read_offset, bufferlist);
    if (unique_run_id != read_unique_run_id) {
      return ErrorType::RUN_ID_MISMATCH;
    }

    SeedBytes read_seed = readSeed(block_offset - read_offset, bufferlist);
    if (m_model.get_seed(block_offset) != read_seed) {
      return ErrorType::SEED_MISMATCH;
    }

    if (std::strncmp(&bufferlist[((block_offset - read_offset) *
                                  m_model.get_block_size()) +
                                 bodyStart()],
                     generate_block(block_offset).c_str() + bodyStart(),
                     m_model.get_block_size() - bodyStart()) != 0) {
      return ErrorType::DATA_MISMATCH;
    }
  } catch (const std::exception& e) {
    return ErrorType::DATA_NOT_FOUND;
  }

  return ErrorType::UNKNOWN;
}

void HeaderedSeededRandomGenerator ::printDebugInformationForBlock(
    uint64_t read_offset, uint64_t block_offset, const bufferlist& bufferlist) {
  ErrorType blockError =
      getErrorTypeForBlock(read_offset, block_offset, bufferlist);

  TimeBytes read_time = 0;
  std::time_t ttp;

  char read_bytes[m_model.get_block_size()];
  char generated_bytes[m_model.get_block_size()];

  if (blockError == ErrorType::DATA_MISMATCH ||
      blockError == ErrorType::UNKNOWN) {
    read_time = readDateTime(block_offset - read_offset, bufferlist);
    std::chrono::system_clock::time_point time_point{
        std::chrono::milliseconds{read_time}};
    ttp = std::chrono::system_clock::to_time_t(time_point);

    std::memcpy(
        &read_bytes,
        &bufferlist[((block_offset - read_offset) * m_model.get_block_size())],
        m_model.get_block_size() - bodyStart());
    std::memcpy(&generated_bytes, generate_block(block_offset).c_str(),
                m_model.get_block_size() - bodyStart());
  }

  std::string error_string;
  switch (blockError) {
    case ErrorType::RUN_ID_MISMATCH: {
      UniqueIdBytes read_unique_run_id =
          readUniqueRunId((block_offset - read_offset), bufferlist);
      error_string = fmt::format(
          "Header (Run ID) mismatch detected at block {} "
          "(byte offset {}) Header expected run id {} but found id {}. "
          "Block data corrupt or not written from this instance of this "
          "application.",
          block_offset, block_offset * m_model.get_block_size(), unique_run_id,
          read_unique_run_id);
    } break;

    case ErrorType::SEED_MISMATCH: {
      SeedBytes read_seed = readSeed((block_offset - read_offset), bufferlist);

      if (m_model.get_seed_offsets(read_seed).size() == 0) {
        error_string = fmt::format(
            "Data (Seed) mismatch detected at block {}"
            " (byte offset {}). Header expected seed {} but found seed {}. "
            "Read data was not from any other recognised block in the object.",
            block_offset, block_offset * m_model.get_block_size(),
            m_model.get_seed(block_offset), read_seed);
      } else {
        std::vector<int> seed_offsets = m_model.get_seed_offsets(read_seed);
        error_string = fmt::format(
            "Data (Seed) mismatch detected at block {}"
            " (byte offset {}). Header expected seed {} but found seed {}."
            " Read data was from a different block(s): {}",
            block_offset, block_offset * m_model.get_block_size(),
            m_model.get_seed(block_offset), read_seed,
            fmt::join(seed_offsets.begin(), seed_offsets.end(), ""));
      }
    } break;

    case ErrorType::DATA_MISMATCH: {
      error_string = fmt::format(
          "Data (Body) mismatch detected at block {}"
          " (byte offset {}). Header data matches, data body does not."
          " Data written at {}\nExpected data: \n{:02x}\nRead data:{:02x}",
          block_offset, block_offset * m_model.get_block_size(),
          std::ctime(&ttp),
          fmt::join(generated_bytes, generated_bytes + m_model.get_block_size(),
                    ""),
          fmt::join(read_bytes, read_bytes + m_model.get_block_size(), ""));
    } break;

    case ErrorType::DATA_NOT_FOUND: {
      uint64_t bufferlist_length = bufferlist.to_str().size();
      error_string = fmt::format(
          "Data (Body) could not be read at block {}"
          " (byte offset {}) offset in bufferlist returned from read: {}"
          " ({} bytes). Returned bufferlist length: {}.",
          block_offset, block_offset * m_model.get_block_size(),
          (block_offset - read_offset),
          (block_offset - read_offset) * m_model.get_block_size(),
          bufferlist_length);
    } break;

    case ErrorType::UNKNOWN:
      [[fallthrough]];

    default: {
      error_string = fmt::format(
          "Data mismatch detected at block {}"
          " (byte offset {}).\nExpected data:\n{:02x}\nRead data:\n{:02x}",
          block_offset, block_offset * m_model.get_block_size(),
          fmt::join(generated_bytes, generated_bytes + m_model.get_block_size(),
                    ""),
          fmt::join(read_bytes, read_bytes + m_model.get_block_size(), ""));
    } break;
  }
  dout(0) << error_string << dendl;
}

void HeaderedSeededRandomGenerator ::printDebugInformationForRange(
    uint64_t read_offset, uint64_t start_block_offset,
    uint64_t range_length_in_blocks, ErrorType rangeError,
    const bufferlist& bufferlist) {
  switch (rangeError) {
    case ErrorType::RUN_ID_MISMATCH:
      printDebugInformationForRunIdMismatchRange(
          read_offset, start_block_offset, range_length_in_blocks, bufferlist);
      break;
    case ErrorType::SEED_MISMATCH:
      printDebugInformationForSeedMismatchRange(
          read_offset, start_block_offset, range_length_in_blocks, bufferlist);
      break;
    case ErrorType::DATA_MISMATCH:
      printDebugInformationDataBodyMismatchRange(
          read_offset, start_block_offset, range_length_in_blocks, bufferlist);
      break;
    case ErrorType::DATA_NOT_FOUND:
      printDebugInformationDataNotFoundRange(
          read_offset, start_block_offset, range_length_in_blocks, bufferlist);
      break;
    case ErrorType::UNKNOWN:
      [[fallthrough]];
    default:
      printDebugInformationCorruptRange(read_offset, start_block_offset,
                                        range_length_in_blocks, bufferlist);
      break;
  }
}

void HeaderedSeededRandomGenerator ::printDebugInformationForRunIdMismatchRange(
    uint64_t read_offset, uint64_t start_block_offset,
    uint64_t range_length_in_blocks, const bufferlist& bufferlist) {
  uint64_t range_start = start_block_offset;
  uint64_t range_length = 0;
  UniqueIdBytes initial_read_unique_run_id =
      readUniqueRunId(start_block_offset - read_offset, bufferlist);
  for (uint64_t i = start_block_offset;
       i < start_block_offset + range_length_in_blocks; i++) {
    ceph_assert(getErrorTypeForBlock(read_offset, i, bufferlist) ==
                ErrorType::RUN_ID_MISMATCH);

    UniqueIdBytes read_unique_run_id =
        readUniqueRunId(i - read_offset, bufferlist);
    if (initial_read_unique_run_id != read_unique_run_id ||
        i == (start_block_offset + range_length_in_blocks - 1)) {
      if (range_length == 1) {
        printDebugInformationForBlock(read_offset, i, bufferlist);
      } else if (range_length > 1) {
        dout(0)
            << fmt::format(
                   "Data (Run ID) Mismatch detected from block {} ({} bytes)"
                   " and spanning a range of {} blocks ({} bytes). "
                   "Expected run id {} for range but found id {}"
                   " for all blocks in range. "
                   "Block data corrupt or not written from this instance of "
                   "this application.",
                   range_start, range_start * m_model.get_block_size(),
                   range_length, range_length * m_model.get_block_size(),
                   unique_run_id, initial_read_unique_run_id)
            << dendl;
      }

      range_start = i;
      range_length = 1;
      initial_read_unique_run_id = read_unique_run_id;
    } else {
      range_length++;
    }
  }

  if (range_length == 1) {
    printDebugInformationForBlock(
        read_offset, start_block_offset + range_length_in_blocks - 1,
        bufferlist);
  } else if (range_length > 1) {
    dout(0) << fmt::format(
                   "Data (Run ID) Mismatch detected from block {}"
                   " ({} bytes) and spanning a range of {} blocks ({} bytes). "
                   "Expected run id {} for range but found id for all blocks "
                   "in range. "
                   "Block data corrupt or not written from this instance of "
                   "this application.",
                   range_start, range_start * m_model.get_block_size(),
                   range_length, range_length * m_model.get_block_size(),
                   unique_run_id, initial_read_unique_run_id)
            << dendl;
  }
}

void HeaderedSeededRandomGenerator ::printDebugInformationForSeedMismatchRange(
    uint64_t read_offset, uint64_t start_block_offset,
    uint64_t range_length_in_blocks, const bufferlist& bufferlist) {
  uint64_t range_start = start_block_offset;
  uint64_t range_length = 0;

  // Assert here if needed, as we can't support values
  // that can't be converted to a signed integer.
  ceph_assert(m_model.get_block_size() <
              (std::numeric_limits<uint64_t>::max() / 2));
  std::optional<int64_t> range_offset = 0;

  for (uint64_t i = start_block_offset;
       i < start_block_offset + range_length_in_blocks; i++) {
    ceph_assert(getErrorTypeForBlock(read_offset, i, bufferlist) ==
                ErrorType::SEED_MISMATCH);
    SeedBytes read_seed = readSeed(i - read_offset, bufferlist);

    std::vector<int> seed_found_offsets = m_model.get_seed_offsets(read_seed);

    if ((seed_found_offsets.size() == 1 &&
         (static_cast<int64_t>(seed_found_offsets.front() - i) ==
          range_offset)) ||
        range_length == 0) {
      if (range_length == 0) {
        range_start = i;
        if (seed_found_offsets.size() > 0) {
          range_offset = seed_found_offsets.front() - i;
        } else {
          range_offset = std::nullopt;
        }
      }
      range_length++;
    } else {
      if (range_length == 1) {
        printDebugInformationForBlock(read_offset, i - 1, bufferlist);
      } else if (range_length > 1 && range_offset.has_value()) {
        dout(0)
            << fmt::format(
                   "Data (Seed) Mismatch detected from block {}"
                   " ({} bytes) and spanning a range of {} blocks ({} bytes). "
                   "Returned data located starting from block {} ({} bytes) "
                   "and spanning a range of {} blocks ({} bytes).",
                   range_start, range_start * m_model.get_block_size(),
                   range_length, range_length * m_model.get_block_size(),
                   static_cast<uint64_t>(*range_offset) + range_start,
                   (static_cast<uint64_t>(*range_offset) + range_start) *
                       m_model.get_block_size(),
                   range_length, range_length * m_model.get_block_size())
            << dendl;
      } else {
        dout(0)
            << fmt::format(
                   "Data (Seed) Mismatch detected from block {}"
                   " ({} bytes) and spanning a range of {} blocks ({} bytes). "
                   "Data seed mismatch spanning a range of {} blocks ({} "
                   "bytes).",
                   range_start, range_start * m_model.get_block_size(),
                   range_length, range_length * m_model.get_block_size(),
                   range_length, range_length * m_model.get_block_size())
            << dendl;
      }
      range_length = 1;
      range_start = i;
      if (seed_found_offsets.size() > 0) {
        range_offset = seed_found_offsets.front() - i;
      } else {
        range_offset = std::nullopt;
      }
    }
  }

  if (range_length == 1) {
    printDebugInformationForBlock(
        read_offset, start_block_offset + range_length_in_blocks - 1,
        bufferlist);
  } else if (range_length > 1 && range_offset.has_value()) {
    dout(0) << fmt::format(
                   "Data (Seed) Mismatch detected from block {} ({} bytes) "
                   "and spanning a range of {} blocks ({} bytes). "
                   "Returned data located starting from block {} ({} bytes) "
                   "and spanning a range of {} blocks ({} bytes).",
                   range_start, range_start * m_model.get_block_size(),
                   range_length, range_length * m_model.get_block_size(),
                   *range_offset + range_start,
                   (*range_offset + range_start) * m_model.get_block_size(),
                   range_length, range_length * m_model.get_block_size())
            << dendl;
  } else {
    dout(0) << fmt::format(
                   "Data (Seed) Mismatch detected from block {} ({} bytes) "
                   "and spanning a range of {} blocks ({} bytes). "
                   "and spanning a range of {} blocks ({} bytes).",
                   range_start, range_start * m_model.get_block_size(),
                   range_length, range_length * m_model.get_block_size(),
                   range_length, range_length * m_model.get_block_size())
            << dendl;
  }
}

void HeaderedSeededRandomGenerator ::printDebugInformationDataBodyMismatchRange(
    uint64_t read_offset, uint64_t start_block_offset,
    uint64_t range_length_in_blocks, const bufferlist& bufferlist) {
  dout(0) << fmt::format(
                 "Data Mismatch detected in blocks from {} to {}. "
                 "Headers look as expected for range, "
                 "but generated data body does not match. "
                 "More information given for individual blocks below.",
                 start_block_offset,
                 start_block_offset + range_length_in_blocks - 1)
          << dendl;

  for (uint64_t i = start_block_offset;
       i < start_block_offset + range_length_in_blocks; i++) {
    printDebugInformationForBlock(read_offset, i, bufferlist);
  }
}

void HeaderedSeededRandomGenerator ::printDebugInformationCorruptRange(
    uint64_t read_offset, uint64_t start_block_offset,
    uint64_t range_length_in_blocks, const bufferlist& bufferlist) {
  dout(0) << fmt::format(
                 "Data Mismatch detected in blocks from {} to {}. "
                 "Headers look as expected for range, "
                 "but generated data body does not match. "
                 "More information given for individual blocks below.",
                 start_block_offset,
                 start_block_offset + range_length_in_blocks - 1)
          << dendl;

  for (uint64_t i = start_block_offset;
       i < start_block_offset + range_length_in_blocks; i++) {
    printDebugInformationForBlock(read_offset, i, bufferlist);
  }
}

void HeaderedSeededRandomGenerator ::printDebugInformationDataNotFoundRange(
    uint64_t read_offset, uint64_t start_block_offset,
    uint64_t range_length_in_blocks, const bufferlist& bufferlist) {
  dout(0) << fmt::format(
                 "Data not found for blocks from {} to {}. "
                 "More information given for individual blocks below.",
                 start_block_offset,
                 start_block_offset + range_length_in_blocks - 1)
          << dendl;

  for (uint64_t i = start_block_offset;
       i < start_block_offset + range_length_in_blocks; i++) {
    printDebugInformationForBlock(read_offset, i, bufferlist);
  }
}

void HeaderedSeededRandomGenerator ::printDebugInformationForOffsets(
    uint64_t read_offset, std::vector<uint64_t> offsets,
    const bufferlist& bufferlist) {
  uint64_t range_start = 0;
  uint64_t range_length = 0;
  ErrorType rangeError = ErrorType::UNKNOWN;

  for (const uint64_t& block_offset : offsets) {
    ErrorType blockError =
        getErrorTypeForBlock(read_offset, block_offset, bufferlist);

    if (range_start == 0 && range_length == 0) {
      range_start = block_offset;
      range_length = 1;
      rangeError = blockError;
    } else if (blockError == rangeError &&
               range_start + range_length == block_offset) {
      range_length++;
    } else {
      if (range_length == 1) {
        printDebugInformationForBlock(read_offset, range_start, bufferlist);
      } else if (range_length > 1) {
        printDebugInformationForRange(read_offset, range_start, range_length,
                                      rangeError, bufferlist);
      }

      range_start = block_offset;
      range_length = 1;
      rangeError = blockError;
    }
  }

  if (range_length == 1) {
    printDebugInformationForBlock(read_offset, range_start, bufferlist);
  } else if (range_length > 1) {
    printDebugInformationForRange(read_offset, range_start, range_length,
                                  rangeError, bufferlist);
  }
}
