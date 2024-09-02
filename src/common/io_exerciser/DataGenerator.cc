// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "DataGenerator.h"

#include "ObjectModel.h"

#include "common/debug.h"
#include "common/dout.h"

#include <chrono>
#include <iostream>
#include <stdexcept>

#define dout_subsys ceph_subsys_rados
#define dout_context g_ceph_context

std::mutex ceph::io_exerciser::data_generation::DataGenerator
  ::DataGenerationSingleton::m_mutex;
ceph::io_exerciser::data_generation::DataGenerator::DataGenerationSingleton
  ceph::io_exerciser::data_generation::DataGenerator::DataGenerationSingleton::m_singletonInstance =
    ceph::io_exerciser::data_generation::DataGenerator::DataGenerationSingleton();

std::unique_ptr<ceph::io_exerciser::data_generation::DataGenerator>
  ceph::io_exerciser::data_generation::DataGenerator::create_generator(
    ceph::io_exerciser::data_generation::GenerationType generationType,
    const ObjectModel& model)
{
  switch(generationType)
  {
    case GenerationType::SeededRandom:
      return std::make_unique<SeededRandomGenerator>(model);
    case GenerationType::HeaderedSeededRandom:
      return std::make_unique<HeaderedSeededRandomGenerator>(model);
    default:
      throw std::invalid_argument("Not yet implemented");
  }

  return nullptr;
}

ceph::io_exerciser::data_generation::DataGenerator
  ::DataGenerationSingleton::DataGenerationSingleton()
{
  m_created = false;
}

ceph::io_exerciser::data_generation::DataGenerator
  ::DataGenerationSingleton::DataGenerationSingleton(uint64_t unique_id)
{
  m_uniqueId = unique_id;
  m_created = true;
}

const ceph::io_exerciser::data_generation
  ::DataGenerator::DataGenerationSingleton& 
  ceph::io_exerciser::data_generation::DataGenerator
    ::DataGenerationSingleton::createSpecificInstance(uint64_t unique_id)
{
  std::scoped_lock lock(m_mutex);
  ceph_assert(!m_singletonInstance.m_created);
  m_singletonInstance = DataGenerationSingleton(unique_id);

  return m_singletonInstance;
}

const ceph::io_exerciser::data_generation::DataGenerator::DataGenerationSingleton&
  ceph::io_exerciser::data_generation::DataGenerator
  ::DataGenerationSingleton::getInstance()
{
  if (!m_singletonInstance.m_created)
  {
    std::scoped_lock lock(m_mutex);
    if (!m_singletonInstance.m_created)
    {
      std::mt19937_64 random_generator = 
        std::mt19937_64(duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch()).count());
      m_singletonInstance = DataGenerator::DataGenerationSingleton(random_generator());
    }
  }
  return m_singletonInstance;
}

const uint64_t ceph::io_exerciser::data_generation::DataGenerator
  ::DataGenerationSingleton::getUniqueId()
{
  return getInstance().m_uniqueId;
}

void ceph::io_exerciser::data_generation
  ::DataGenerator::generate_wrong_data(uint64_t offset, uint64_t length,
                                       ceph::bufferlist& retlist)
{
  uint64_t block_size = m_model.get_block_size();
  char buffer[block_size];
  for (uint64_t block_offset = offset;
       block_offset < offset + length;
       block_offset++)
  { 
    std::memset(buffer, 0, block_size);
    retlist.append(ceph::bufferptr(buffer, block_size));
  }
}

bool ceph::io_exerciser::data_generation
  ::DataGenerator::validate(ceph::bufferlist& bufferlist, uint64_t offset, uint64_t length)
{
  ceph::bufferlist comparison_list;
  generate_data(offset, length, comparison_list);
  return bufferlist.contents_equal(comparison_list);
}

#include <bitset>

ceph::bufferptr ceph::io_exerciser::data_generation
  ::SeededRandomGenerator::generate_block(uint64_t block_offset)
{
  uint64_t block_size = m_model.get_block_size();
  char buffer[block_size];
  
  std::mt19937_64 random_generator(m_model.get_seed(block_offset));
  uint64_t rand1 = random_generator();
  uint64_t rand2 = random_generator();

  constexpr size_t generation_length = sizeof(uint64_t);

  for (uint64_t i = 0; i < block_size; i+=(2*generation_length), rand1++, rand2--)
  {
    std::memcpy(buffer + i, &rand1, generation_length);
    std::memcpy(buffer + i + generation_length, &rand2, generation_length);
  }

  size_t remainingBytes = block_size % (generation_length * 2);
  if (remainingBytes > generation_length)
  {
    size_t remainingBytes2 = remainingBytes - generation_length;
    std::memcpy(buffer + block_size - remainingBytes, &rand1, remainingBytes);
    std::memcpy(buffer + block_size - remainingBytes2, &rand2, remainingBytes2);
  }
  else if (remainingBytes > 0)
  {
    std::memcpy(buffer + block_size - remainingBytes, &rand1, remainingBytes);
  }

  return ceph::bufferptr(buffer, block_size);
}

void ceph::io_exerciser::data_generation
  ::SeededRandomGenerator::generate_data(uint64_t offset, uint64_t length, ceph::bufferlist& retlist)
{
  ceph_assert(retlist.length() == 0);

  for (uint64_t block_offset = offset; block_offset < offset + length; block_offset++)
  {
    retlist.append(generate_block(block_offset));
  }
}

ceph::bufferptr ceph::io_exerciser::data_generation
  ::HeaderedSeededRandomGenerator::generate_block(uint64_t block_offset)
{
  UniqueIdBytes unique_run_id = DataGenerator::DataGenerationSingleton::getUniqueId();
  SeedBytes seed = m_model.get_seed(block_offset);
  TimeBytes current_time = duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();

  ceph::bufferptr bufferptr = SeededRandomGenerator::generate_block(block_offset);

  std::memcpy(bufferptr.c_str(), &unique_run_id + uniqueIdStart(), uniqueIdLength());
  std::memcpy(bufferptr.c_str() + seedStart(), &seed, seedLength());
  std::memcpy(bufferptr.c_str() + timeStart(), &current_time, timeLength());

  return bufferptr;
}

void ceph::io_exerciser::data_generation
  ::HeaderedSeededRandomGenerator::generate_wrong_data(uint64_t offset,
                                                       uint64_t length,
                                                       ceph::bufferlist& retlist)
{
  ceph_assert(retlist.length() == 0);
  ceph_assert(m_model.get_block_size() >= headerLength());

  ceph::bufferptr bufferptr;
  for (uint64_t block_offset = offset; block_offset < offset + length; block_offset++)
  {
      UniqueIdBytes unique_run_id = DataGenerator::DataGenerationSingleton::getUniqueId();
      SeedBytes seed = m_model.get_seed(block_offset-1);
      TimeBytes current_time = duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch()).count();

      bufferptr = generate_block(block_offset-1);

      std::memcpy(bufferptr.c_str(), &unique_run_id + uniqueIdStart(), uniqueIdLength());
      std::memcpy(bufferptr.c_str() + seedStart(), &seed, seedLength());
      std::memcpy(bufferptr.c_str() + timeStart(), &current_time, timeLength());

      retlist.append(std::move(bufferptr));
  }
}

const ceph::io_exerciser::data_generation::HeaderedSeededRandomGenerator::UniqueIdBytes
  ceph::io_exerciser::data_generation
    ::HeaderedSeededRandomGenerator::readUniqueRunId(uint64_t block_offset,
                                                     const ceph::bufferlist& bufferlist)
{
  UniqueIdBytes read_unique_run_id = 0;
  std::memcpy(&read_unique_run_id,
              &bufferlist[(block_offset * m_model.get_block_size()) + uniqueIdStart()],
              uniqueIdLength());
  return read_unique_run_id;
}

const ceph::io_exerciser::data_generation::HeaderedSeededRandomGenerator::SeedBytes
  ceph::io_exerciser::data_generation
    ::HeaderedSeededRandomGenerator::readSeed(uint64_t block_offset,
                                              const ceph::bufferlist& bufferlist)
{
  SeedBytes read_seed = 0;
  std::memcpy(&read_seed,
              &bufferlist[(block_offset * m_model.get_block_size()) + seedStart()],
              seedLength());
  return read_seed;
}

const ceph::io_exerciser::data_generation::HeaderedSeededRandomGenerator::TimeBytes
  ceph::io_exerciser::data_generation
  ::HeaderedSeededRandomGenerator::readDateTime(uint64_t block_offset,
                                                const ceph::bufferlist& bufferlist)
{
  TimeBytes read_time = 0;
  std::memcpy(&read_time,
              &bufferlist[(block_offset * m_model.get_block_size()) + timeStart()],
              timeLength());
  return read_time;
}

bool ceph::io_exerciser::data_generation
  ::HeaderedSeededRandomGenerator::validate(ceph::bufferlist& bufferlist,
                                            uint64_t offset, uint64_t length)
{
  std::vector<uint64_t> invalid_block_offsets;

  for (uint64_t block_offset = offset; block_offset < offset + length; block_offset++)
  {
    bool valid_block
      = validate_block(block_offset,
                       (bufferlist.c_str() + ((block_offset - offset) *
                       m_model.get_block_size())));
    if (!valid_block)
    {
      invalid_block_offsets.push_back(block_offset);
    }
  }

  if (!invalid_block_offsets.empty())
  {
    printDebugInformationForOffsets(offset, invalid_block_offsets, bufferlist);
  }

  return invalid_block_offsets.empty();
}

bool ceph::io_exerciser::data_generation
  ::HeaderedSeededRandomGenerator::validate_block(uint64_t block_offset,
                                                  const char* buffer_start)
{
  // We validate the block matches what we generate byte for byte, however we ignore the time section of the header
  ceph::bufferptr bufferptr = generate_block(block_offset);
  bool valid = strncmp(bufferptr.c_str(), buffer_start, timeStart()) == 0;
  valid = valid ? strncmp(bufferptr.c_str() + timeEnd(),
                          buffer_start + timeEnd(),
                          m_model.get_block_size() - timeEnd()) == 0 : valid;
  return valid;
}
 
const ceph::io_exerciser::data_generation::HeaderedSeededRandomGenerator::ErrorType
  ceph::io_exerciser::data_generation::HeaderedSeededRandomGenerator
    ::getErrorTypeForBlock(uint64_t read_offset, uint64_t block_offset,
                           const ceph::bufferlist& bufferlist)
{
  try
  {
    UniqueIdBytes read_unique_run_id = readUniqueRunId(block_offset - read_offset,
                                                       bufferlist);
    if (DataGenerationSingleton::getUniqueId() != read_unique_run_id)
    {
      return ErrorType::RUN_ID_MISMATCH;
    }

    SeedBytes read_seed = readSeed(block_offset - read_offset, bufferlist);
    if (m_model.get_seed(block_offset) != read_seed)
    {
      return ErrorType::SEED_MISMATCH;
    }

    if (std::strncmp(&bufferlist[((block_offset - read_offset) *
                      m_model.get_block_size()) + bodyStart()],
                     generate_block(block_offset).c_str() + bodyStart(),
                     m_model.get_block_size() - bodyStart()) != 0)
    {
      return ErrorType::DATA_MISMATCH;
    }
  }
  catch(const std::exception& e)
  {
    return ErrorType::DATA_NOT_FOUND;
  }

  return ErrorType::UNKNOWN;
}

void ceph::io_exerciser::data_generation::HeaderedSeededRandomGenerator
  ::printDebugInformationForBlock(uint64_t read_offset, uint64_t block_offset,
                                  const ceph::bufferlist& bufferlist)
{
  ErrorType blockError = getErrorTypeForBlock(read_offset, block_offset, bufferlist);

  TimeBytes read_time = 0;
  std::time_t ttp;

  char read_bytes[m_model.get_block_size()];
  char generated_bytes[m_model.get_block_size()];

  if (blockError == ErrorType::DATA_MISMATCH || blockError == ErrorType::UNKNOWN)
  {
    read_time = readDateTime(block_offset, bufferlist);
    std::chrono::system_clock::time_point time_point{std::chrono::milliseconds{read_time}};
    ttp = std::chrono::system_clock::to_time_t(time_point);

    std::memcpy(&read_bytes,
                &bufferlist[((block_offset - read_offset) * m_model.get_block_size())],
                m_model.get_block_size() - bodyStart());
    std::memcpy(&generated_bytes,
                generate_block(block_offset).c_str(),
                m_model.get_block_size() - bodyStart());
  }
  std::stringstream ss;
  switch(blockError)
  {
    case ErrorType::RUN_ID_MISMATCH:
    {
      UniqueIdBytes read_unique_run_id = readUniqueRunId((block_offset - read_offset),
                                                          bufferlist);
      ss << "Header (Run ID) mismatch detected at block " << block_offset 
         << " (byte offset " << block_offset * m_model.get_block_size() << ")."
         << " Header expected run id " << DataGenerationSingleton::getUniqueId()
         << " but found id " << read_unique_run_id
         << ". Block data corrupt or not written from this instance of this application.";
    }
    break;
    
    case ErrorType::SEED_MISMATCH:
    {
      SeedBytes read_seed = readSeed((block_offset - read_offset), bufferlist);

      if (m_model.get_seed_offsets(read_seed).size() == 0)
      {
        ss << "Data (Seed) mismatch detected at block " << block_offset
           << " (byte offset " << block_offset * m_model.get_block_size() << ")."
           << " Header expected seed " << m_model.get_seed(block_offset)
           << " but found seed " << read_seed
           << ". Read data was not from any other recognised block in the object.";
      }
      else
      {
        ss << "Data (Seed) mismatch detected at block " << block_offset
           << " (byte offset " << block_offset * m_model.get_block_size() << ")."
           << " Header expected seed " << m_model.get_seed(block_offset)
           << " but found seed " << read_seed
           << ". Read data was from a different block(s): "
           << m_model.get_seed_offsets(read_seed);
      }
    }
    break;

    case ErrorType::DATA_MISMATCH:
    {
      ss << "Data (Body) mismatch detected at block " << block_offset
         << " (byte offset " << block_offset * m_model.get_block_size() << ")."
         << " Header data matches, data body does not."
         << " Data written at " << std::ctime(&ttp)
         << "\nExpected data: " << std::endl;
      ss << std::hex << std::setw(2) << std::setfill('0');
      for (uint64_t i = headerLength();
           i < m_model.get_block_size() - headerLength(); i++)
      {
        ss << static_cast<int>(generated_bytes[i]);
      }
      dout(0) << dendl;
      ss << " Read data: " << std::endl;
      for (uint64_t i = headerLength();
           i < m_model.get_block_size() - headerLength(); i++)
      {
        ss << static_cast<int>(read_bytes[i]);
      }
    }
    break;

    case ErrorType::DATA_NOT_FOUND:
    {
      uint64_t bufferlist_length = bufferlist.to_str().size();
      ss << "Data (Body) could not be read at block " << block_offset
         << " (byte offset " << block_offset * m_model.get_block_size() << ")"
         << " offset in bufferlist returned from read: " << (block_offset - read_offset)
         << " (" << (block_offset - read_offset) * m_model.get_block_size() << " bytes)."
         << " Returned bufferlist length: " << bufferlist_length;
    }
    break;

    case ErrorType::UNKNOWN:
      [[ fallthrough ]];

    default:
    {
      ss << "Data mismatch detected at block " << block_offset
         << " (byte offset " << block_offset * m_model.get_block_size() << ")."
         << "\nExpected data: " << std::endl;
      ss << std::hex << std::setw(2) << std::setfill('0');
      for (uint64_t i = 0; i < m_model.get_block_size(); i++)
      {
        ss << static_cast<int>(generated_bytes[i]);
      }
      ss << std::endl << "Read data: " << std::endl;
      for (uint64_t i = 0; i < m_model.get_block_size(); i++)
      {
        ss << static_cast<int>(read_bytes[i]);
      }
    }
    break;
  }
  dout(0) << ss.str() << dendl;
}

void ceph::io_exerciser::data_generation::HeaderedSeededRandomGenerator
  ::printDebugInformationForRange(uint64_t read_offset,
                                  uint64_t start_block_offset,
                                  uint64_t range_length_in_blocks,
                                  ErrorType rangeError,
                                  const ceph::bufferlist& bufferlist)
{
  switch(rangeError)
  {
  case ErrorType::RUN_ID_MISMATCH:
    printDebugInformationForRunIdMismatchRange(read_offset, start_block_offset,
                                               range_length_in_blocks, bufferlist);
    break;
  case ErrorType::SEED_MISMATCH:
    printDebugInformationForSeedMismatchRange(read_offset, start_block_offset,
                                              range_length_in_blocks, bufferlist);
    break;
  case ErrorType::DATA_MISMATCH:
    printDebugInformationDataBodyMismatchRange(read_offset, start_block_offset,
                                               range_length_in_blocks, bufferlist);
    break;
  case ErrorType::DATA_NOT_FOUND:
    printDebugInformationDataNotFoundRange(read_offset, start_block_offset,
                                           range_length_in_blocks, bufferlist);
    break;
  case ErrorType::UNKNOWN:
    [[ fallthrough ]];
  default:
    printDebugInformationCorruptRange(read_offset, start_block_offset,
                                      range_length_in_blocks, bufferlist);
    break;
  }
}

void ceph::io_exerciser::data_generation::HeaderedSeededRandomGenerator
  ::printDebugInformationForRunIdMismatchRange(uint64_t read_offset,
                                               uint64_t start_block_offset,
                                               uint64_t range_length_in_blocks,
                                               const ceph::bufferlist& bufferlist)
{
  uint64_t range_start = start_block_offset;
  uint64_t range_length = 0;
  UniqueIdBytes initial_read_unique_run_id = readUniqueRunId(start_block_offset,
                                                             bufferlist);
  for (uint64_t i = start_block_offset;
       i < start_block_offset + range_length_in_blocks; i++)
  {
    ceph_assert(getErrorTypeForBlock(read_offset, i, bufferlist)
                == ErrorType::RUN_ID_MISMATCH);

    UniqueIdBytes read_unique_run_id = readUniqueRunId(i, bufferlist);
    if (initial_read_unique_run_id != read_unique_run_id ||
        i == (start_block_offset + range_length_in_blocks - 1))
    {
      if (range_length == 1)
      {
        printDebugInformationForBlock(read_offset, i, bufferlist);
      }
      else if (range_length > 1)
      {
        dout(0) << "Data (Run ID) Mismatch detected from block " << range_start
                << " (" << range_start * m_model.get_block_size() << " bytes)"
                << " and spanning a range of " << range_length << " blocks"
                << "(" << range_length * m_model.get_block_size() << " bytes). "
                << "Expected run id " << DataGenerationSingleton::getUniqueId()
                << " for range but found id " << initial_read_unique_run_id
                << " for all blocks in range. "
                << "Block data corrupt or not written from this instance of this application."
                << dendl;
      }

      range_start = i;
      range_length = 1;
      initial_read_unique_run_id = read_unique_run_id;
    }
    else
    {
      range_length++; 
    }
  }

  if (range_length == 1)
  {
    printDebugInformationForBlock(read_offset,
                                  start_block_offset + range_length_in_blocks - 1,
                                  bufferlist);
  }
  else if (range_length > 1)
  {
    dout(0) << "Data (Run ID) Mismatch detected from block " << range_start
            << " (" << range_start * m_model.get_block_size() << " bytes) "
            << "and spanning a range of " << range_length << " blocks " 
            << "(" << range_length * m_model.get_block_size() << " bytes). "
            << "Expected run id " << DataGenerationSingleton::getUniqueId()
            << " for range but found id " << initial_read_unique_run_id
            << " for all blocks in range. "
            << "Block data corrupt or not written from this instance of this application."
            << dendl;
  }
}

void ceph::io_exerciser::data_generation::HeaderedSeededRandomGenerator
  ::printDebugInformationForSeedMismatchRange(uint64_t read_offset,
                                              uint64_t start_block_offset,
                                              uint64_t range_length_in_blocks,
                                              const ceph::bufferlist& bufferlist)
{
  uint64_t range_start = start_block_offset;
  uint64_t range_length = 0;

  // Assert here if needed, as we can't support values
  // that can't be converted to a signed integer.
  ceph_assert(m_model.get_block_size() < (std::numeric_limits<uint64_t>::max() / 2));
  int64_t range_offset = 0;

  for (uint64_t i = start_block_offset;
       i < start_block_offset + range_length_in_blocks; i++)
  {
    ceph_assert(getErrorTypeForBlock(read_offset, i, bufferlist)
                == ErrorType::SEED_MISMATCH);
    SeedBytes read_seed = readSeed(i, bufferlist);

    std::vector<int> seed_found_offsets = m_model.get_seed_offsets(read_seed);

    if ((seed_found_offsets.size() == 1 &&
        (static_cast<int64_t>(seed_found_offsets.front() - i) == range_offset)) || 
        range_length == 0)
    {
      if (range_length == 0)
      {
        range_start = i;
        range_offset = seed_found_offsets.front() - i;
      }
      range_length++;
    }
    else
    {
      if (range_length == 1)
      {
        printDebugInformationForBlock(read_offset, i - 1, bufferlist);
      }
      else if (range_length > 1)
      {
        dout(0) << "Data (Seed) Mismatch detected from block " << range_start
                << " (" << range_start * m_model.get_block_size() << " bytes) "
                << "and spanning a range of " << range_length << " blocks "
                << "(" << range_length * m_model.get_block_size() << " bytes). "
                << "Returned data located starting from block "
                << static_cast<uint64_t>(range_offset) + range_start
                << " (" << (static_cast<uint64_t>(range_offset) + range_start)
                            * m_model.get_block_size() << " bytes) "
                << "and spanning a range of " << range_length << " blocks "
                << "(" << range_length * m_model.get_block_size() << " bytes)."
                << dendl;
      }
      range_length = 1;
      range_start = i;
      range_offset = seed_found_offsets.front() - i;
    }
  }

  if (range_length == 1)
  {
    printDebugInformationForBlock(read_offset,
                                  start_block_offset + range_length_in_blocks - 1,
                                  bufferlist);
  }
  else if (range_length > 1)
  {
    dout(0) << "Data (Seed) Mismatch detected from block " << range_start
            << " (" << range_start * m_model.get_block_size() << " bytes) "
            << "and spanning a range of " << range_length << " blocks "
            << "(" << range_length * m_model.get_block_size() << " bytes). "
            << "Returned data located starting from block "
            << range_offset + range_start
            << " (" << (range_offset + range_start) * m_model.get_block_size()
            << " bytes) and spanning a range of " << range_length << " blocks "
            << "(" << range_length * m_model.get_block_size() << " bytes)."
            << dendl;
  }
}

void ceph::io_exerciser::data_generation::HeaderedSeededRandomGenerator
::printDebugInformationDataBodyMismatchRange(uint64_t read_offset,
                                             uint64_t start_block_offset,
                                             uint64_t range_length_in_blocks,
                                             const ceph::bufferlist& bufferlist)
{
  dout(0) << "Data Mismatch detected in blocks "
          << "from " << start_block_offset
          << " to " << start_block_offset + range_length_in_blocks - 1 << ". "
          << "Headers look as expected for range, "
          << "but generated data body does not match. "
          << "More information given for individual blocks below." << dendl;

  for (uint64_t i = start_block_offset;
       i < start_block_offset + range_length_in_blocks; i++)
  {
    printDebugInformationForBlock(read_offset, i, bufferlist);
  }
}

void ceph::io_exerciser::data_generation::HeaderedSeededRandomGenerator
  ::printDebugInformationCorruptRange(uint64_t read_offset,
                                      uint64_t start_block_offset,
                                      uint64_t range_length_in_blocks,
                                      const ceph::bufferlist& bufferlist)
{
  dout(0) << "Data Mismatch detected in blocks "
  << "from " << start_block_offset 
  << " to " << start_block_offset + range_length_in_blocks - 1 << ". "
  << "Headers look as expected for range, but generated data body does not match."
  << " More information given for individual blocks below." << dendl;

  for (uint64_t i = start_block_offset;
       i < start_block_offset + range_length_in_blocks; i++)
  {
    printDebugInformationForBlock(read_offset, i, bufferlist);
  }
}

void ceph::io_exerciser::data_generation::HeaderedSeededRandomGenerator
  ::printDebugInformationDataNotFoundRange(uint64_t read_offset,
                                           uint64_t start_block_offset,
                                           uint64_t range_length_in_blocks,
                                           const ceph::bufferlist& bufferlist)
{
  dout(0) << "Data not found for blocks "
          << "from " << start_block_offset
          << " to " << start_block_offset + range_length_in_blocks - 1 << ". "
          << "More information given for individual blocks below." << dendl;

  for (uint64_t i = start_block_offset; i < start_block_offset + range_length_in_blocks; i++)
  {
    printDebugInformationForBlock(read_offset, i, bufferlist);
  }
}

void ceph::io_exerciser::data_generation::HeaderedSeededRandomGenerator
  ::printDebugInformationForOffsets(uint64_t read_offset,
                                    std::vector<uint64_t> offsets,
                                    const ceph::bufferlist& bufferlist)
{
  uint64_t range_start = 0;
  uint64_t range_length = 0;
  ErrorType rangeError = ErrorType::UNKNOWN;

  for (const uint64_t& block_offset : offsets)
  {
    ErrorType blockError = getErrorTypeForBlock(read_offset, block_offset,
                                                bufferlist);

    if (range_start == 0 && range_length == 0)
    {
      range_start = block_offset;
      range_length = 1;
      rangeError = blockError;
    }
    else if (blockError == rangeError &&
             range_start + range_length == block_offset)
{
      range_length++;
    }
    else
    {
      if (range_length == 1)
      {
        printDebugInformationForBlock(read_offset, range_start, bufferlist);
      }
      else if (range_length > 1)
      {
        printDebugInformationForRange(read_offset, range_start, range_length,
                                      rangeError, bufferlist);
      }

      range_start = block_offset;
      range_length = 1;
      rangeError = blockError;
    }
  }

  if (range_length == 1)
  {
    printDebugInformationForBlock(read_offset, range_start, bufferlist);
  }
  else if (range_length > 1)
  {
    printDebugInformationForRange(read_offset, range_start, range_length,
                                  rangeError, bufferlist);
  }
}