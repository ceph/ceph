// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2018 Indian Institute of Science <office.ece@iisc.ac.in>
 *
 * Author: Myna Vajha <mynaramana@gmail.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#include <errno.h>
#include <stdlib.h>

#include "crush/CrushWrapper.h"
#include "include/stringify.h"
#include "erasure-code/clay/ErasureCodeClay.h"
#include "global/global_context.h"
#include "common/config_proxy.h"
#include "gtest/gtest.h"

// FIXME: Clay is not yet supported in new EC.
IGNORE_DEPRECATED

using namespace std;

TEST(ErasureCodeClay, sanity_check_k)
{
  ErasureCodeClay clay(g_conf().get_val<std::string>("erasure_code_dir"));
  ErasureCodeProfile profile;
  profile["k"] = "1";
  profile["m"] = "1";
  ostringstream errors;
  EXPECT_EQ(-EINVAL, clay.init(profile, &errors));
  EXPECT_NE(std::string::npos, errors.str().find("must be >= 2"));
}

TEST(ErasureCodeClay, DISABLED_encode_decode)
{
  ostringstream errors;
  ErasureCodeClay clay(g_conf().get_val<std::string>("erasure_code_dir"));
  ErasureCodeProfile profile;
  profile["k"] = "2";
  profile["m"] = "2";
  int r= clay.init(profile, &cerr);
  EXPECT_EQ(0, r);

#define LARGE_ENOUGH 2048
  bufferptr in_ptr(buffer::create_page_aligned(LARGE_ENOUGH));
  in_ptr.zero();
  in_ptr.set_length(0);
  const char *payload =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  in_ptr.append(payload, strlen(payload));
  bufferlist in;
  in.push_back(in_ptr);
  int want_to_encode[] = { 0, 1, 2, 3 };
  map<int, bufferlist> encoded;
  EXPECT_EQ(0, clay.encode(set<int>(want_to_encode, want_to_encode+4),
			   in,
			   &encoded));
  EXPECT_EQ(4u, encoded.size());
  unsigned length =  encoded[0].length();
  EXPECT_EQ(0, memcmp(encoded[0].c_str(), in.c_str(), length));
  EXPECT_EQ(0, memcmp(encoded[1].c_str(), in.c_str() + length,
                      in.length() - length));


  // all chunks are available
  {
    int want_to_decode[] = { 0, 1 };
    map<int, bufferlist> decoded;
    EXPECT_EQ(0, clay._decode(set<int>(want_to_decode, want_to_decode+2),
			      encoded,
			      &decoded));
    EXPECT_EQ(2u, decoded.size()); 
    EXPECT_EQ(length, decoded[0].length());
    EXPECT_EQ(0, memcmp(decoded[0].c_str(), in.c_str(), length));
    EXPECT_EQ(0, memcmp(decoded[1].c_str(), in.c_str() + length,
			in.length() - length));
  }

  // check all two chunks missing possibilities and recover them
  for (int i=1; i<4; i++) {
    for (int j=0; j<i; j++) {
      map<int, bufferlist> degraded = encoded;
      degraded.erase(j);
      degraded.erase(i);
      EXPECT_EQ(2u, degraded.size());
      int want_to_decode[] = {j,i};
      map<int, bufferlist> decoded;
      EXPECT_EQ(0, clay._decode(set<int>(want_to_decode, want_to_decode+2),
				degraded,
				&decoded));
      EXPECT_EQ(4u, decoded.size()); 
      EXPECT_EQ(length, decoded[j].length());
      EXPECT_EQ(0, memcmp(decoded[j].c_str(), encoded[j].c_str(), length));
      EXPECT_EQ(0, memcmp(decoded[i].c_str(), encoded[i].c_str(), length));
    }
  }
  //check for all one chunk missing possibilities
  int sc_size = length/clay.sub_chunk_no;
  int avail[] = {0,1,2,3};
  for (int i=0; i < 4; i++) {
    set<int> want_to_read;
    want_to_read.insert(i);
    set<int> available(avail, avail+4);
    available.erase(i);
    map<int, vector<pair<int,int>>> minimum;
    EXPECT_EQ(0, clay.minimum_to_decode(want_to_read, available, &minimum));
    map<int, bufferlist> helper;
    for (map<int, vector<pair<int,int>>>::iterator h=minimum.begin(); h!= minimum.end(); ++h) {
      for(vector<pair<int,int>>::iterator ind=h->second.begin(); ind != h->second.end(); ++ind) {
	bufferlist temp;
	temp.substr_of(encoded[h->first], ind->first*sc_size, ind->second*sc_size);
	helper[h->first].append(temp);
      }
    }
    for (map<int, vector<pair<int,int>>>::iterator h=minimum.begin(); h!= minimum.end(); ++h) {
      EXPECT_EQ(length/clay.q, helper[h->first].length());
    }
    EXPECT_EQ(3u, helper.size());
    map<int, bufferlist> decoded;
    EXPECT_EQ(0, clay.decode(want_to_read, helper, &decoded, length));
    EXPECT_EQ(1u, decoded.size());
    EXPECT_EQ(0, memcmp(decoded[i].c_str(), encoded[i].c_str(), length));
  }
}


TEST(ErasureCodeClay, DISABLED_encode_decode_aloof_nodes)
{
  ostringstream errors;
  ErasureCodeClay clay(g_conf().get_val<std::string>("erasure_code_dir"));
  ErasureCodeProfile profile;
  profile["k"] = "3";
  profile["m"] = "3";
  profile["d"] = "4";
  int r= clay.init(profile, &cerr);
  EXPECT_EQ(0, r);

#define LARGE_ENOUGH 2048
  bufferptr in_ptr(buffer::create_page_aligned(LARGE_ENOUGH));
  in_ptr.zero();
  in_ptr.set_length(0);
  const char *payload =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  in_ptr.append(payload, strlen(payload));
  bufferlist in;
  in.push_back(in_ptr);
  int want_to_encode[] = { 0, 1, 2, 3, 4, 5 };
  map<int, bufferlist> encoded;
  EXPECT_EQ(0, clay.encode(set<int>(want_to_encode, want_to_encode+6),
			   in,
			   &encoded));
  EXPECT_EQ(6u, encoded.size());
  unsigned length =  encoded[0].length();
  if (in.length() < length) {
    EXPECT_EQ(0, memcmp(encoded[0].c_str(), in.c_str(), in.length()));
  } else if (in.length() <= 2*length ) {
    EXPECT_EQ(0, memcmp(encoded[0].c_str(), in.c_str(), in.length()));
    EXPECT_EQ(0, memcmp(encoded[1].c_str(), in.c_str()+length, in.length()-length));
  } else {
    EXPECT_EQ(1, in.length() <= 3*length);
    EXPECT_EQ(0, memcmp(encoded[0].c_str(), in.c_str(), in.length()));
    EXPECT_EQ(0, memcmp(encoded[1].c_str(), in.c_str()+length, length));
    EXPECT_EQ(0, memcmp(encoded[2].c_str(), in.c_str()+2*length, in.length()-2*length));
  }

  // all chunks are available
  {
    int want_to_decode[] = { 0, 1, 2 };
    map<int, bufferlist> decoded;
    EXPECT_EQ(0, clay._decode(set<int>(want_to_decode, want_to_decode+3),
			      encoded,
			      &decoded));
    EXPECT_EQ(3u, decoded.size()); 
    EXPECT_EQ(length, decoded[0].length());
    EXPECT_EQ(0, memcmp(decoded[0].c_str(), encoded[0].c_str(), length));
    EXPECT_EQ(0, memcmp(decoded[1].c_str(), encoded[1].c_str(), length));
    EXPECT_EQ(0, memcmp(decoded[2].c_str(), encoded[2].c_str(), length));
  }

  // check all three chunks missing possibilities and recover them
  for (int i=2; i<6; i++) {
    for (int j=1; j<i; j++) {
      for(int k=0; k<j; k++) {
	map<int, bufferlist> degraded = encoded;
	degraded.erase(k);
	degraded.erase(j);
	degraded.erase(i);
	EXPECT_EQ(3u, degraded.size());
	int want_to_decode[] = {k,j,i};
	map<int, bufferlist> decoded;
	EXPECT_EQ(0, clay._decode(set<int>(want_to_decode, want_to_decode+3),
				  degraded,
				  &decoded));
	EXPECT_EQ(6u, decoded.size()); 
	EXPECT_EQ(length, decoded[j].length());
	EXPECT_EQ(0, memcmp(decoded[k].c_str(), encoded[k].c_str(), length));
	EXPECT_EQ(0, memcmp(decoded[j].c_str(), encoded[j].c_str(), length));
	EXPECT_EQ(0, memcmp(decoded[i].c_str(), encoded[i].c_str(), length));
      }
    }
  }
  //check for all one chunk missing possibilities
  int sc_size = length/clay.sub_chunk_no;
  int avail[] = {0,1,2,3,4,5};
  for (int i=0; i < 6; i++) {
    vector<pair<int,int>> repair_subchunks;
    map<int, vector<pair<int,int>>> minimum;
    set<int> want_to_read;
    want_to_read.insert(i);
    set<int> available(avail, avail+6);
    available.erase(i);
    clay.minimum_to_decode(want_to_read, available, &minimum);
    map<int, bufferlist> helper;
    for (map<int, vector<pair<int,int>>>::iterator h=minimum.begin(); h!= minimum.end(); ++h) {
      for(vector<pair<int,int>>::iterator ind=h->second.begin(); ind != h->second.end(); ++ind) {
	bufferlist temp;
	temp.substr_of(encoded[h->first], ind->first*sc_size, ind->second*sc_size);
	helper[h->first].append(temp);
      }
    }  
    for (map<int, vector<pair<int,int>>>::iterator h=minimum.begin(); h!= minimum.end(); ++h) {
      EXPECT_EQ(length/clay.q, helper[h->first].length());
    }
    EXPECT_EQ((unsigned)clay.d, helper.size());
    map<int, bufferlist> decoded;
    EXPECT_EQ(0, clay.decode(want_to_read, helper, &decoded, length));
    EXPECT_EQ(1u, decoded.size());
    EXPECT_EQ(0, memcmp(decoded[i].c_str(), encoded[i].c_str(), length));
  }
}

TEST(ErasureCodeClay, DISABLED_encode_decode_shortening_case)
{
  ostringstream errors;
  ErasureCodeClay clay(g_conf().get_val<std::string>("erasure_code_dir"));
  ErasureCodeProfile profile;
  profile["k"] = "4";
  profile["m"] = "3";
  profile["d"] = "5";
  int r= clay.init(profile, &cerr);
  EXPECT_EQ(0, r);

  EXPECT_EQ(2, clay.q);
  EXPECT_EQ(4, clay.t);
  EXPECT_EQ(1, clay.nu);

  bufferptr in_ptr(buffer::create_page_aligned(LARGE_ENOUGH));
  in_ptr.zero();
  in_ptr.set_length(0);
  const char *payload =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  in_ptr.append(payload, strlen(payload));
  bufferlist in;
  in.push_back(in_ptr);
  int want_to_encode[] = { 0, 1, 2, 3, 4, 5, 6 };
  map<int, bufferlist> encoded;
  EXPECT_EQ(0, clay.encode(set<int>(want_to_encode, want_to_encode+7),
			   in,
			   &encoded));
  EXPECT_EQ(7u, encoded.size());
  unsigned length =  encoded[0].length();
  if (in.length() < length) {
    EXPECT_EQ(0, memcmp(encoded[0].c_str(), in.c_str(), in.length()));
  } else if (in.length() <= 2*length) {
    EXPECT_EQ(0, memcmp(encoded[0].c_str(), in.c_str(), in.length()));
    EXPECT_EQ(0, memcmp(encoded[1].c_str(), in.c_str()+length, in.length()-length));
  } else if (in.length() <= 3*length) {
    EXPECT_EQ(0, memcmp(encoded[0].c_str(), in.c_str(), in.length()));
    EXPECT_EQ(0, memcmp(encoded[1].c_str(), in.c_str()+length, length));
    EXPECT_EQ(0, memcmp(encoded[2].c_str(), in.c_str()+2*length, in.length()-2*length));
  } else {
    EXPECT_EQ(1, in.length() <= 4*length);
    EXPECT_EQ(0, memcmp(encoded[0].c_str(), in.c_str(), in.length()));
    EXPECT_EQ(0, memcmp(encoded[1].c_str(), in.c_str()+length, length));
    EXPECT_EQ(0, memcmp(encoded[2].c_str(), in.c_str()+2*length, length));
    EXPECT_EQ(0, memcmp(encoded[3].c_str(), in.c_str()+3*length, in.length()-3*length));
  }

  // all chunks are available
  {
    int want_to_decode[] = { 0, 1, 2, 3 };
    map<int, bufferlist> decoded;
    EXPECT_EQ(0, clay._decode(set<int>(want_to_decode, want_to_decode+4),
			      encoded,
			      &decoded));
    EXPECT_EQ(4u, decoded.size()); 
    EXPECT_EQ(length, decoded[0].length());
    EXPECT_EQ(0, memcmp(decoded[0].c_str(), encoded[0].c_str(), length));
    EXPECT_EQ(0, memcmp(decoded[1].c_str(), encoded[1].c_str(), length));
    EXPECT_EQ(0, memcmp(decoded[2].c_str(), encoded[2].c_str(), length));
    EXPECT_EQ(0, memcmp(decoded[3].c_str(), encoded[3].c_str(), length));
  }

  // check all three chunks missing possibilities and recover them
  for (int i=2; i<7; i++) {
    for (int j=1; j<i; j++) {
      for(int k=0; k<j; k++) {
	map<int, bufferlist> degraded = encoded;
	degraded.erase(k);
	degraded.erase(j);
	degraded.erase(i);
	EXPECT_EQ(4u, degraded.size());
	int want_to_decode[] = {k,j,i};
	map<int, bufferlist> decoded;
	EXPECT_EQ(0, clay._decode(set<int>(want_to_decode, want_to_decode+3),
				  degraded,
				  &decoded));
	EXPECT_EQ(7u, decoded.size()); 
	EXPECT_EQ(length, decoded[j].length());
	EXPECT_EQ(0, memcmp(decoded[k].c_str(), encoded[k].c_str(), length));
	EXPECT_EQ(0, memcmp(decoded[j].c_str(), encoded[j].c_str(), length));
	EXPECT_EQ(0, memcmp(decoded[i].c_str(), encoded[i].c_str(), length));
      }
    }
  }
  //check for all one chunk missing possibilities
  int sc_size = length/clay.sub_chunk_no;
  int avail[] = {0,1,2,3,4,5,6};
  for (int i=0; i < 7; i++) {
    vector<pair<int,int>> repair_subchunks;
    map<int, vector<pair<int,int>>> minimum;
    set<int> want_to_read;
    want_to_read.insert(i);
    set<int> available(avail, avail+7);
    available.erase(i);
    clay.minimum_to_decode(want_to_read, available, &minimum);
    map<int, bufferlist> helper;
    for (map<int, vector<pair<int,int>>>::iterator h=minimum.begin(); h!= minimum.end(); ++h) {
      for(vector<pair<int,int>>::iterator ind=h->second.begin(); ind != h->second.end(); ++ind) {
	bufferlist temp;
	temp.substr_of(encoded[h->first], ind->first*sc_size, ind->second*sc_size);
	helper[h->first].append(temp);
      }
    }  
    for (map<int, vector<pair<int,int>>>::iterator h=minimum.begin(); h!= minimum.end(); ++h) {
      EXPECT_EQ(length/clay.q, helper[h->first].length());
    }
    EXPECT_EQ(static_cast<size_t>(clay.d), helper.size());
    map<int, bufferlist> decoded;
    EXPECT_EQ(0, clay.decode(want_to_read, helper, &decoded, length));
    EXPECT_EQ(1u, decoded.size());
    EXPECT_EQ(length, decoded[i].length());
    EXPECT_EQ(0, memcmp(decoded[i].c_str(), encoded[i].c_str(), length));
  }
}

TEST(ErasureCodeClay, minimum_to_decode)
{
  ErasureCodeClay clay(g_conf().get_val<std::string>("erasure_code_dir"));
  ErasureCodeProfile profile;
  profile["k"] = "2";
  profile["m"] = "2";
  EXPECT_EQ(0, clay.init(profile, &cerr));

  //
  // If trying to read nothing, the minimum is empty.
  //
  {
    set<int> want_to_read;
    set<int> available_chunks;
    set<int> minimum;

    EXPECT_EQ(0, clay._minimum_to_decode(want_to_read,
					 available_chunks,
					 &minimum));
    EXPECT_TRUE(minimum.empty());
  }
  //
  // There is no way to read a chunk if none are available.
  //
  {
    set<int> want_to_read;
    set<int> available_chunks;
    set<int> minimum;

    want_to_read.insert(0);

    EXPECT_EQ(-EIO, clay._minimum_to_decode(want_to_read,
					    available_chunks,
					    &minimum));
  }
  //
  // Reading a subset of the available chunks is always possible.
  //
  {
    set<int> want_to_read;
    set<int> available_chunks;
    set<int> minimum;

    want_to_read.insert(0);
    available_chunks.insert(0);

    EXPECT_EQ(0, clay._minimum_to_decode(want_to_read,
					 available_chunks,
					 &minimum));
    EXPECT_EQ(want_to_read, minimum);
  }
  //
  // There is no way to read a missing chunk if there is less than k
  // chunks available.
  //
  {
    set<int> want_to_read;
    set<int> available_chunks;
    set<int> minimum;

    want_to_read.insert(0);
    want_to_read.insert(1);
    available_chunks.insert(0);

    EXPECT_EQ(-EIO, clay._minimum_to_decode(want_to_read,
					    available_chunks,
					    &minimum));
  }
  //
  // When chunks are not available, the minimum can be made of any
  // chunks. For instance, to read 1 and 3 below the minimum could be
  // 2 and 3 which may seem better because it contains one of the
  // chunks to be read. But it won't be more efficient than retrieving
  // 0 and 2 instead because, in both cases, the decode function will
  // need to run the same recovery operation and use the same amount
  // of CPU and memory.
  //
  {
    set<int> want_to_read;
    set<int> available_chunks;
    set<int> minimum;

    want_to_read.insert(1);
    want_to_read.insert(3);
    available_chunks.insert(0);
    available_chunks.insert(2);
    available_chunks.insert(3);

    EXPECT_EQ(0, clay._minimum_to_decode(want_to_read,
					 available_chunks,
					 &minimum));
    EXPECT_EQ(2u, minimum.size());
    EXPECT_EQ(0u, minimum.count(3));
  }
}

TEST(ErasureCodeClay, encode)
{
  ErasureCodeClay clay(g_conf().get_val<std::string>("erasure_code_dir"));
  ErasureCodeProfile profile;
  profile["k"] = "2";
  profile["m"] = "2";
  EXPECT_EQ(0, clay.init(profile, &cerr));

  unsigned aligned_object_size = clay.get_chunk_size(1) * 2 * 2;
  {
    //
    // When the input bufferlist needs to be padded because
    // it is not properly aligned, it is padded with zeros.
    //
    bufferlist in;
    map<int,bufferlist> encoded;
    int want_to_encode[] = { 0, 1, 2, 3 };
    int trail_length = 1;
    in.append(string(aligned_object_size + trail_length, 'X'));
    EXPECT_EQ(0, clay.encode(set<int>(want_to_encode, want_to_encode+4),
			     in,
			     &encoded));
    EXPECT_EQ(4u, encoded.size());
    char *last_chunk = encoded[1].c_str();
    int length =encoded[1].length();
    EXPECT_EQ('X', last_chunk[0]);
    EXPECT_EQ('\0', last_chunk[length - trail_length]);
  }

  {
    //
    // When only the first chunk is required, the encoded map only
    // contains the first chunk. Although the clay encode
    // internally allocated a buffer because of padding requirements
    // and also computes the coding chunks, they are released before
    // the return of the method, as shown when running the tests thru
    // valgrind (there is no leak).
    //
    bufferlist in;
    map<int,bufferlist> encoded;
    set<int> want_to_encode;
    want_to_encode.insert(0);
    int trail_length = 1;
    in.append(string(aligned_object_size + trail_length, 'X'));
    EXPECT_EQ(0, clay.encode(want_to_encode, in, &encoded));
    EXPECT_EQ(1u, encoded.size());
  }
}

TEST(ErasureCodeClay, create_rule)
{
  std::unique_ptr<CrushWrapper> c = std::make_unique<CrushWrapper>();
  c->create();
  int root_type = 2;
  c->set_type_name(root_type, "root");
  int host_type = 1;
  c->set_type_name(host_type, "host");
  int osd_type = 0;
  c->set_type_name(osd_type, "osd");

  int rootno;
  c->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
		root_type, 0, NULL, NULL, &rootno);
  c->set_item_name(rootno, "default");

  map<string,string> loc;
  loc["root"] = "default";

  int num_host = 4;
  int num_osd = 5;
  int osd = 0;
  for (int h=0; h<num_host; ++h) {
    loc["host"] = string("host-") + stringify(h);
    for (int o=0; o<num_osd; ++o, ++osd) {
      c->insert_item(g_ceph_context, osd, 1.0, string("osd.") + stringify(osd), loc);
    }
  }

  c->finalize();

  {
    stringstream ss;
    ErasureCodeClay clay(g_conf().get_val<std::string>("erasure_code_dir"));
    ErasureCodeProfile profile;
    profile["k"] = "2";
    profile["m"] = "2";
    EXPECT_EQ(0, clay.init(profile, &cerr));
    int ruleid = clay.create_rule("myrule", *c, &ss);
    EXPECT_EQ(0, ruleid);
    EXPECT_EQ(-EEXIST, clay.create_rule("myrule", *c, &ss));
    //
    // the minimum that is expected from the created rule is to
    // successfully map get_chunk_count() devices from the crushmap,
    // at least once.
    //
    vector<__u32> weight(c->get_max_devices(), 0x10000);
    vector<int> out;
    int x = 0;
    c->do_rule(ruleid, x, out,   clay.get_chunk_count(), weight, 0);
    ASSERT_EQ(out.size(), clay.get_chunk_count());
    for (unsigned i=0; i<out.size(); ++i)
      ASSERT_NE(CRUSH_ITEM_NONE, out[i]);
  }
  {
    stringstream ss;
    ErasureCodeClay clay(g_conf().get_val<std::string>("erasure_code_dir"));
    ErasureCodeProfile profile;
    profile["k"] = "2";
    profile["m"] = "2";
    profile["crush-root"] = "BAD";
    EXPECT_EQ(0, clay.init(profile, &cerr));
    EXPECT_EQ(-ENOENT, clay.create_rule("otherrule", *c, &ss));
    EXPECT_EQ("root item BAD does not exist", ss.str());
  }
  {
    stringstream ss;
    ErasureCodeClay clay(g_conf().get_val<std::string>("erasure_code_dir"));
    ErasureCodeProfile profile;
    profile["k"] = "2";
    profile["m"] = "2";
    profile["crush-failure-domain"] = "WORSE";
    EXPECT_EQ(0, clay.init(profile, &cerr));
    EXPECT_EQ(-EINVAL, clay.create_rule("otherrule", *c, &ss));
    EXPECT_EQ("unknown type WORSE", ss.str());
  }
}

END_IGNORE_DEPRECATED

/* 
 * Local Variables:
 * compile-command: "cd ../.. ;
 *   make -j4 unittest_erasure_code_clay &&
 *   valgrind --tool=memcheck \
 *      ./unittest_erasure_code_clay \
 *      --gtest_filter=*.* --log-to-stderr=true --debug-osd=20"
 * End:
 */
