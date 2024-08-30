// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "DataGenerator.h"

#include "Model.h"

#include <stdexcept>
#include <iostream>

std::unique_ptr<Ceph::DataGenerator::DataGenerator> Ceph::DataGenerator::DataGenerator::create_generator(Ceph::DataGenerator::GenerationType generationType, const ObjectModel& model)
{
  switch(generationType)
  {
    case Ceph::DataGenerator::GenerationType::Zero:
    case Ceph::DataGenerator::GenerationType::One:
      throw std::invalid_argument("Not yet implemented");
    case Ceph::DataGenerator::GenerationType::SeededRandom:
      return std::make_unique<Ceph::DataGenerator::SeededRandomGenerator>(model);
    case Ceph::DataGenerator::GenerationType::HeaderedSeededRandom:
      return std::make_unique<Ceph::DataGenerator::HeaderedSeededRandomGenerator>(model);
    case Ceph::DataGenerator::GenerationType::ZeroParity:
    case Ceph::DataGenerator::GenerationType::OneParity:
    case Ceph::DataGenerator::GenerationType::ZeroPrimaryChunk:
    case Ceph::DataGenerator::GenerationType::OnePrimaryChunk:
    case Ceph::DataGenerator::GenerationType::ZeroSecondaryChunk:
    case Ceph::DataGenerator::GenerationType::OneSecondaryChunk:
    case Ceph::DataGenerator::GenerationType::Mixed:
    default:
      throw std::invalid_argument("Not yet implemented");
  }

  return nullptr;
}

bool Ceph::DataGenerator::DataGenerator::validate(const ceph::bufferlist& bufferlist, uint64_t offset, uint64_t length)
{
  ceph::bufferlist comparison_list;
  generate_data(offset, length, comparison_list);
  return bufferlist.contents_equal(comparison_list);
}

void Ceph::DataGenerator::ZeroGenerator::generate_data(uint64_t offset, uint64_t length, ceph::bufferlist& retlist)
{
  
}

void Ceph::DataGenerator::OneGenerator::generate_data(uint64_t offset, uint64_t length, ceph::bufferlist& retlist)
{
  
}

ceph::bufferptr Ceph::DataGenerator::SeededRandomGenerator::generate_block(uint64_t block_offset)
{
  uint64_t block_size = m_model.get_block_size();
  ceph::bufferptr bufferptr = ceph::bufferptr(block_size);

  random_generator.seed(m_model.get_seed(block_offset));

  for (uint64_t i = 0; i < block_size; i++)
  {
    bufferptr[i] = (random_generator());
  }

  return bufferptr;
}

void Ceph::DataGenerator::SeededRandomGenerator::generate_data(uint64_t offset, uint64_t length, ceph::bufferlist& retlist)
{
  ceph_assert(retlist.length() == 0);

  ceph::bufferptr bufferptr;
  for (uint64_t block_offset = offset; block_offset < offset + length; block_offset++)
  {
    retlist.append(generate_block(block_offset));
  }

  // std::ios_base::fmtflags f( std::cout.flags() );
  // std::cout << "We generated this buffer: " << std::hex << std::setw(2) << std::setfill('0');
  // for (uint64_t i = 0; i < length; i++)
  // {
  //   for (uint64_t j = 0; j < m_model.get_block_size(); j++)
  //   {
  //     std::cout << static_cast<int>(retlist.c_str()[(i * m_model.get_block_size()) + j]);
  //   }
  // }
  // std::cout << std::endl;
  // std::cout.flags(f);
}

// I'm lazy and just copy and pasted this for now - I can make better use of inheritance here
void Ceph::DataGenerator::HeaderedSeededRandomGenerator::generate_data(uint64_t offset, uint64_t length, ceph::bufferlist& retlist)
{
  ceph_assert(retlist.length() == 0);
  ceph_assert(m_model.get_block_size() > 8);

  ceph::bufferptr bufferptr;
  for (uint64_t block_offset = offset; block_offset < offset + length; block_offset++)
  {
    
      uint64_t seed = m_model.get_seed(block_offset);
      bufferptr = generate_block(block_offset);

      std::memcpy(bufferptr.c_str(), &seed, 8);

      retlist.append(bufferptr);
  }

  // std::ios_base::fmtflags f( std::cout.flags() );
  // std::cout << "We generated this buffer: " << std::hex << std::setw(2) << std::setfill('0');
  // for (uint64_t i = 0; i < length; i++)
  // {
  //   for (uint64_t j = 0; j < m_model.get_block_size(); j++)
  //   {
  //     std::cout << static_cast<int>(retlist.c_str()[(i * m_model.get_block_size()) + j]);
  //   }
  // }
  // std::cout << std::endl;
  // std::cout.flags(f);
}
 
bool Ceph::DataGenerator::HeaderedSeededRandomGenerator::validate(const ceph::bufferlist& bufferlist, uint64_t offset, uint64_t length)
{
  bool valid = Ceph::DataGenerator::DataGenerator::validate(bufferlist, offset, length);
  if (!valid)
  {
    for (uint64_t i = 0; i < length; i++)
    {
      uint64_t found_seed = 0;
      for (int j = 0; j < 7; j++)
      {
        found_seed |= (uint8_t)bufferlist[(i * m_model.get_block_size()) + j];
      }

      // std::cout << "Expected seed " << m_model.get_seed(i) << " at offset " << (i * m_model.get_block_size()) << " and found " << found_seed << std::endl;
    }
  }

  return valid;
}

void Ceph::DataGenerator::ZeroParityGenerator::generate_data(uint64_t offset, uint64_t length, ceph::bufferlist& retlist)
{
  // Parities can be generated through the libraries below - although if we have a bug in the partity calculations
  // This does not test the parity calculations, as any bugs will be replicated in Ceph and this test tool as we are using the same library
  // What it does test is the erasure coding, to check there aren't any cases where we drop data in special cases and combinations
}

void Ceph::DataGenerator::OneParityGenerator::generate_data(uint64_t offset, uint64_t length, ceph::bufferlist& retlist)
{
  // Parities can be generated through the libraries below - although if we have a bug in the partity calculations
  // This does not test the parity calculations, as any bugs will be replicated in Ceph and this test tool as we are using the same library
  // What it does test is the erasure coding, to check there aren't any cases where we drop data in special cases and combinations
}

void Ceph::DataGenerator::ZeroPrimaryChunkDataGenerator::generate_data(uint64_t offset, uint64_t length, ceph::bufferlist& retlist)
{
  
}

void Ceph::DataGenerator::OnePrimaryChunkDataGenerator::generate_data(uint64_t offset, uint64_t length, ceph::bufferlist& retlist)
{
  
}

void Ceph::DataGenerator::ZeroSecondaryChunkDataGenerator::generate_data(uint64_t offset, uint64_t length, ceph::bufferlist& retlist)
{
  
}

void Ceph::DataGenerator::OneSecondaryChunkDataGenerator::generate_data(uint64_t offset, uint64_t length, ceph::bufferlist& retlist)
{
  
}

void Ceph::DataGenerator::MultiGenerator::generate_data(uint64_t offset, uint64_t length, ceph::bufferlist& retlist)
{
  // The idea for this generator is to have it be able to select from any of the above generators and be able to know which
  // model it chose and validate using that models validator. This will let us run combinations of tests and various data patterns.
}



// Code example of how to encode and decode using erasure plugins
// TEST(ErasureCodeShec, decode2_4)
// {
//   //init
//   ErasureCodeShecTableCache tcache;
//   ErasureCodeShec* shec = new ErasureCodeShecReedSolomonVandermonde(
// 				  tcache,
// 				  ErasureCodeShec::MULTIPLE);
//   ErasureCodeProfile *profile = new ErasureCodeProfile();
//   (*profile)["plugin"] = "shec";
//   (*profile)["technique"] = "";
//   (*profile)["crush-failure-domain"] = "osd";
//   (*profile)["k"] = "4";
//   (*profile)["m"] = "3";
//   (*profile)["c"] = "2";
//   shec->init(*profile, &cerr);

//   //encode
//   bufferlist in;
//   set<int> want_to_encode;
//   map<int, bufferlist> encoded;

//   in.append("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//length = 62
// 	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//124
// 	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//186
// 	    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"//248
//   );
//   for (unsigned int i = 0; i < shec->get_chunk_count(); ++i) {
//     want_to_encode.insert(i);
//   }

//   int r = shec->encode(want_to_encode, in, &encoded);
//   EXPECT_EQ(0, r);
//   EXPECT_EQ(shec->get_chunk_count(), encoded.size());
//   EXPECT_EQ(shec->get_chunk_size(in.length()), encoded[0].length());

//   //decode
//   int want_to_decode[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
//   map<int, bufferlist> decoded;

//   // cannot recover
//   bufferlist out;
//   map<int, bufferlist> degraded;
//   degraded[0] = encoded[0];

//   r = shec->_decode(set<int>(want_to_decode, want_to_decode + 2), degraded,
// 		    &decoded);
//   EXPECT_EQ(-1, r);

//   delete shec;
//   delete profile;
// }
