// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include <algorithm>
#include <boost/algorithm/string/trim.hpp>
#include <boost/tokenizer.hpp>
#include <boost/uuid/uuid.hpp>             // uuid class
#include <boost/uuid/uuid_generators.hpp>  // generators
#include <boost/uuid/uuid_io.hpp>          // streaming operators etc.
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

#include "common/ceph_json.h"
#include "global/global_init.h"
#include "include/compat.h"

#include "lazy_omap_stats_test.h"

using namespace std;

void LazyOmapStatsTest::init(const int argc, const char** argv)
{
  int ret = rados.init("admin");
  if (ret < 0) {
    ret = -ret;
    cerr << "Failed to initialise rados! Error: " << ret << " " << strerror(ret)
         << endl;
    exit(ret);
  }

  ret = rados.conf_parse_argv(argc, argv);
  if (ret < 0) {
    ret = -ret;
    cerr << "Failed to parse command line config options! Error: " << ret << " "
         << strerror(ret) << endl;
    exit(ret);
  }

  rados.conf_parse_env(NULL);
  if (ret < 0) {
    ret = -ret;
    cerr << "Failed to parse environment! Error: " << ret << " "
         << strerror(ret) << endl;
    exit(ret);
  }

  rados.conf_read_file(NULL);
  if (ret < 0) {
    ret = -ret;
    cerr << "Failed to read config file! Error: " << ret << " " << strerror(ret)
         << endl;
    exit(ret);
  }

  ret = rados.connect();
  if (ret < 0) {
    ret = -ret;
    cerr << "Failed to connect to running cluster! Error: " << ret << " "
         << strerror(ret) << endl;
    exit(ret);
  }

  string command = R"(
    {
      "prefix": "osd pool create",
      "pool": ")" + conf.pool_name +
                   R"(",
      "pool_type": "replicated",
      "size": )" + to_string(conf.replica_count) +
                   R"(
    })";
  librados::bufferlist inbl;
  string output;
  ret = rados.mon_command(command, inbl, nullptr, &output);
  if (output.length()) cout << output << endl;
  if (ret < 0) {
    ret = -ret;
    cerr << "Failed to create pool! Error: " << ret << " " << strerror(ret)
         << endl;
    exit(ret);
  }

  ret = rados.ioctx_create(conf.pool_name.c_str(), io_ctx);
  if (ret < 0) {
    ret = -ret;
    cerr << "Failed to create ioctx! Error: " << ret << " " << strerror(ret)
         << endl;
    exit(ret);
  }

  get_pool_id(conf.pool_name);
}

void LazyOmapStatsTest::shutdown()
{
  rados.pool_delete(conf.pool_name.c_str());
  rados.shutdown();
}

void LazyOmapStatsTest::write_omap(const string& object_name)
{
  librados::bufferlist bl;
  int ret = io_ctx.write_full(object_name, bl);
  if (ret < 0) {
    ret = -ret;
    cerr << "Failed to create object! Error: " << ret << " " << strerror(ret)
         << endl;
    exit(ret);
  }
  ret = io_ctx.omap_set(object_name, payload);
  if (ret < 0) {
    ret = -ret;
    cerr << "Failed to write omap payload! Error: " << ret << " "
         << strerror(ret) << endl;
    exit(ret);
  }
  cout << "Wrote " << conf.keys << " omap keys of " << conf.payload_size
       << " bytes to "
       << "the " << object_name << " object" << endl;
}

const string LazyOmapStatsTest::get_name() const
{
  boost::uuids::uuid uuid = boost::uuids::random_generator()();
  return boost::uuids::to_string(uuid);
}

void LazyOmapStatsTest::write_many(uint how_many)
{
  for (uint i = 0; i < how_many; i++) {
    write_omap(get_name());
  }
}

void LazyOmapStatsTest::create_payload()
{
  librados::bufferlist Lorem;
  Lorem.append(
      "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do "
      "eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut "
      "enim ad minim veniam, quis nostrud exercitation ullamco laboris "
      "nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in "
      "reprehenderit in voluptate velit esse cillum dolore eu fugiat "
      "nulla pariatur. Excepteur sint occaecat cupidatat non proident, "
      "sunt in culpa qui officia deserunt mollit anim id est laborum.");
  conf.payload_size = Lorem.length();
  conf.total_bytes = conf.keys * conf.payload_size * conf.how_many;
  conf.total_keys = conf.keys * conf.how_many;
  uint i = 0;
  for (i = 1; i < conf.keys + 1; ++i) {
    payload[get_name()] = Lorem;
  }
  cout << "Created payload with " << conf.keys << " keys of "
       << conf.payload_size
       << " bytes each. Total size in bytes = " << conf.keys * conf.payload_size
       << endl;
}

void LazyOmapStatsTest::scrub()
{
  cout << "Scrubbing" << endl;

  cout << "Before scrub stamps:" << endl;
  string target_pool(conf.pool_id);
  target_pool.append(".");
  bool target_pool_found = false;
  map<string, string> before_scrub = get_scrub_stamps();
  for (auto [pg, stamp] : before_scrub) {
    cout << "pg = " << pg << " stamp = " << stamp << endl;
    if (pg.rfind(target_pool, 0) == 0) {
        target_pool_found = true;
    }
  }
  if (!target_pool_found) {
    cout << "Error: Target pool " << conf.pool_name << ":" << conf.pool_id
         << " not found!" << endl;
    exit(2); // ENOENT
  }
  cout << endl;

  // Short sleep to make sure the new pool is visible
  sleep(5);

  string command = R"({"prefix": "osd deep-scrub", "who": "all"})";
  auto output = get_output(command);
  cout << output << endl;

  cout << "Waiting for deep-scrub to complete..." << endl;
  while (sleep(1) == 0) {
    cout << "Current scrub stamps:" <<  endl;
    bool complete = true;
    map<string, string> current_stamps = get_scrub_stamps();
    for (auto [pg, stamp] : current_stamps) {
        cout << "pg = " << pg << " stamp = " << stamp << endl;
        if (stamp == before_scrub[pg]) {
          // See if stamp for each pg has changed
          // If not, we haven't completed the deep-scrub
          complete = false;
        }
    }
    cout << endl;
    if (complete) {
      break;
    }
  }
  cout << "Scrubbing complete" << endl;
}

const int LazyOmapStatsTest::find_matches(string& output, regex& reg) const
{
  sregex_iterator cur(output.begin(), output.end(), reg);
  uint x = 0;
  for (auto end = std::sregex_iterator(); cur != end; ++cur) {
    cout << (*cur)[1].str() << endl;
    x++;
  }
  return x;
}

const string LazyOmapStatsTest::get_output(const string command,
                                           const bool silent,
                                           const CommandTarget target)
{
  librados::bufferlist inbl, outbl;
  string output;
  int ret = 0;
  if (target == CommandTarget::TARGET_MON) {
    ret = rados.mon_command(command, inbl, &outbl, &output);
  } else {
    ret = rados.mgr_command(command, inbl, &outbl, &output);
  }
  if (output.length() && !silent) {
    cout << output << endl;
  }
  if (ret < 0) {
    ret = -ret;
    cerr << "Failed to get " << command << "! Error: " << ret << " "
         << strerror(ret) << endl;
    exit(ret);
  }
  return string(outbl.c_str(), outbl.length());
}

void LazyOmapStatsTest::get_pool_id(const string& pool)
{
  cout << R"(Querying pool id)" << endl;

  string command = R"({"prefix": "osd pool ls", "detail": "detail", "format": "json"})";
  librados::bufferlist inbl, outbl;
  auto output = get_output(command, false, CommandTarget::TARGET_MON);
  JSONParser parser;
  parser.parse(output.c_str(), output.size());
  for (const auto& pool : parser.get_array_elements()) {
    JSONParser parser2;
    parser2.parse(pool.c_str(), static_cast<int>(pool.size()));
    auto* obj = parser2.find_obj("pool_name");
    if (obj->get_data().compare(conf.pool_name) == 0) {
      obj = parser2.find_obj("pool_id");
      conf.pool_id = obj->get_data();
    }
  }
  if (conf.pool_id.empty()) {
    cout << "Failed to find pool ID for pool " << conf.pool_name << "!" << endl;
    exit(2);    // ENOENT
  } else {
    cout << "Found pool ID: " << conf.pool_id << endl;
  }
}

map<string, string> LazyOmapStatsTest::get_scrub_stamps() {
  map<string, string> stamps;
  string command = R"({"prefix": "pg dump", "format": "json"})";
  auto output = get_output(command);
  JSONParser parser;
  parser.parse(output.c_str(), output.size());
  auto* obj = parser.find_obj("pg_map")->find_obj("pg_stats");
  for (auto pg = obj->find_first(); !pg.end(); ++pg) {
    stamps.insert({(*pg)->find_obj("pgid")->get_data(),
                  (*pg)->find_obj("last_deep_scrub_stamp")->get_data()});
  }
  return stamps;
}

void LazyOmapStatsTest::check_one()
{
  string full_output = get_output();
  cout << full_output << endl;
  regex reg(
      "\n((PG_STAT[\\s\\S]*)\n)OSD_STAT"); // Strip OSD_STAT table so we don't find matches there
  smatch match;
  regex_search(full_output, match, reg);
  auto truncated_output = match[1].str();
  cout << truncated_output << endl;
  reg = regex(
      "\n"
      R"(([0-9,s].*\s)" +
      to_string(conf.keys) +
      R"(\s.*))"
      "\n");

  cout << "Checking number of keys " << conf.keys << endl;
  cout << "Found the following lines" << endl;
  cout << "*************************" << endl;
  uint result = find_matches(truncated_output, reg);
  cout << "**********************" << endl;
  cout << "Found " << result << " matching line(s)" << endl;
  uint total = result;

  reg = regex(
      "\n"
      R"(([0-9,s].*\s)" +
      to_string(conf.payload_size * conf.keys) +
      R"(\s.*))"
      "\n");
  cout << "Checking number of bytes "
       << conf.payload_size * conf.keys << endl;
  cout << "Found the following lines" << endl;
  cout << "*************************" << endl;
  result = find_matches(truncated_output, reg);
  cout << "**********************" << endl;
  cout << "Found " << result << " matching line(s)" << endl;

  total += result;
  if (total != 6) {
    cout << "Error: Found " << total << " matches, expected 6! Exiting..."
         << endl;
    exit(22);  // EINVAL
  }
  cout << "check_one successful. Found " << total << " matches as expected"
       << endl;
}

const int LazyOmapStatsTest::find_index(string& haystack, regex& needle,
                                        string label) const
{
  smatch match;
  regex_search(haystack, match, needle);
  auto line = match[1].str();
  boost::algorithm::trim(line);
  boost::char_separator<char> sep{" "};
  boost::tokenizer<boost::char_separator<char>> tok(line, sep);
  vector<string> tokens(tok.begin(), tok.end());
  auto it = find(tokens.begin(), tokens.end(), label);
  if (it != tokens.end()) {
    return distance(tokens.begin(), it);
  }

  cerr << "find_index failed to find index for " << label << endl;
  exit(2);    // ENOENT
  return -1;  // Unreachable
}

const uint LazyOmapStatsTest::tally_column(const uint omap_bytes_index,
                                           const string& table,
                                           bool header) const
{
  istringstream buffer(table);
  string line;
  uint64_t total = 0;
  while (std::getline(buffer, line)) {
    if (header) {
      header = false;
      continue;
    }
    boost::char_separator<char> sep{" "};
    boost::tokenizer<boost::char_separator<char>> tok(line, sep);
    vector<string> tokens(tok.begin(), tok.end());
    total += stoi(tokens.at(omap_bytes_index));
  }

  return total;
}

void LazyOmapStatsTest::check_column(const int index, const string& table,
                                     const string& type, bool header) const
{
  uint expected;
  string errormsg;
  if (type.compare("bytes") == 0) {
    expected = conf.total_bytes;
    errormsg = "Error. Got unexpected byte count!";
  } else {
    expected = conf.total_keys;
    errormsg = "Error. Got unexpected key count!";
  }
  uint sum = tally_column(index, table, header);
  cout << "Got: " << sum << " Expected: " << expected << endl;
  if (sum != expected) {
    cout << errormsg << endl;
    exit(22);  // EINVAL
  }
}

index_t LazyOmapStatsTest::get_indexes(regex& reg, string& output) const
{
  index_t indexes;
  indexes.byte_index = find_index(output, reg, "OMAP_BYTES*");
  indexes.key_index = find_index(output, reg, "OMAP_KEYS*");

  return indexes;
}

void LazyOmapStatsTest::check_pg_dump()
{
  cout << R"(Checking "pg dump" output)" << endl;

  string dump_output = get_output();
  cout << dump_output << endl;

  regex reg(
      "\n"
      R"((PG_STAT\s.*))"
      "\n");
  index_t indexes = get_indexes(reg, dump_output);

  reg =
      "\n"
      R"((PG_STAT[\s\S]*))"
      "\n +\n[0-9]";
  smatch match;
  regex_search(dump_output, match, reg);
  auto table = match[1].str();

  cout << "Checking bytes" << endl;
  check_column(indexes.byte_index, table, string("bytes"));

  cout << "Checking keys" << endl;
  check_column(indexes.key_index, table, string("keys"));

  cout << endl;
}

void LazyOmapStatsTest::check_pg_dump_summary()
{
  cout << R"(Checking "pg dump summary" output)" << endl;

  string command = R"({"prefix": "pg dump", "dumpcontents": ["summary"]})";
  string dump_output = get_output(command);
  cout << dump_output << endl;

  regex reg(
      "\n"
      R"((PG_STAT\s.*))"
      "\n");
  index_t indexes = get_indexes(reg, dump_output);

  reg =
      "\n"
      R"((sum\s.*))"
      "\n";
  smatch match;
  regex_search(dump_output, match, reg);
  auto table = match[1].str();

  cout << "Checking bytes" << endl;
  check_column(indexes.byte_index, table, string("bytes"), false);

  cout << "Checking keys" << endl;
  check_column(indexes.key_index, table, string("keys"), false);
  cout << endl;
}

void LazyOmapStatsTest::check_pg_dump_pgs()
{
  cout << R"(Checking "pg dump pgs" output)" << endl;

  string command = R"({"prefix": "pg dump", "dumpcontents": ["pgs"]})";
  string dump_output = get_output(command);
  cout << dump_output << endl;

  regex reg(R"(^(PG_STAT\s.*))"
            "\n");
  index_t indexes = get_indexes(reg, dump_output);

  reg = R"(^(PG_STAT[\s\S]*))"
        "\n\n";
  smatch match;
  regex_search(dump_output, match, reg);
  auto table = match[1].str();

  cout << "Checking bytes" << endl;
  check_column(indexes.byte_index, table, string("bytes"));

  cout << "Checking keys" << endl;
  check_column(indexes.key_index, table, string("keys"));
  cout << endl;
}

void LazyOmapStatsTest::check_pg_dump_pools()
{
  cout << R"(Checking "pg dump pools" output)" << endl;

  string command = R"({"prefix": "pg dump", "dumpcontents": ["pools"]})";
  string dump_output = get_output(command);
  cout << dump_output << endl;

  regex reg(R"(^(POOLID\s.*))"
            "\n");
  index_t indexes = get_indexes(reg, dump_output);

  reg =
      "\n"
      R"(()" +
      conf.pool_id +
      R"(\s.*))"
      "\n";
  smatch match;
  regex_search(dump_output, match, reg);
  auto line = match[1].str();

  cout << "Checking bytes" << endl;
  check_column(indexes.byte_index, line, string("bytes"), false);

  cout << "Checking keys" << endl;
  check_column(indexes.key_index, line, string("keys"), false);
  cout << endl;
}

void LazyOmapStatsTest::check_pg_ls()
{
  cout << R"(Checking "pg ls" output)" << endl;

  string command = R"({"prefix": "pg ls"})";
  string dump_output = get_output(command);
  cout << dump_output << endl;

  regex reg(R"(^(PG\s.*))"
            "\n");
  index_t indexes = get_indexes(reg, dump_output);

  reg = R"(^(PG[\s\S]*))"
        "\n\n";
  smatch match;
  regex_search(dump_output, match, reg);
  auto table = match[1].str();

  cout << "Checking bytes" << endl;
  check_column(indexes.byte_index, table, string("bytes"));

  cout << "Checking keys" << endl;
  check_column(indexes.key_index, table, string("keys"));
  cout << endl;
}

void LazyOmapStatsTest::wait_for_active_clean()
{
  cout << "Waiting for active+clean" << endl;

  int index = -1;
  regex reg(R"(PG_STAT[^\n]*\n)");
  string command = R"({"prefix": "pg dump"})";
  int num_not_clean;
  do {
    string dump_output = get_output(command, true);
    if (index == -1) {
      regex ireg(
          "\n"
          R"((PG_STAT\s.*))"
          "\n");
      index = find_index(dump_output, ireg, "STATE");
    }
    smatch match;
    regex_search(dump_output, match, reg);
    istringstream buffer(match[1].str());
    string line;
    num_not_clean = 0;
    while (std::getline(buffer, line)) {
      if (line.compare(0, 1, "P") == 0) continue;
      boost::char_separator<char> sep{" "};
      boost::tokenizer<boost::char_separator<char>> tok(line, sep);
      vector<string> tokens(tok.begin(), tok.end());
      num_not_clean += tokens.at(index).compare("active+clean");
    }
    cout << "." << flush;
    this_thread::sleep_for(chrono::milliseconds(250));
  } while (num_not_clean);

  cout << endl;
}

const int LazyOmapStatsTest::run(const int argc, const char** argv)
{
  init(argc, argv);
  create_payload();
  wait_for_active_clean();
  write_omap(get_name());
  scrub();
  check_one();

  write_many(conf.how_many - 1);  // Since we already wrote one
  scrub();
  check_pg_dump();
  check_pg_dump_summary();
  check_pg_dump_pgs();
  check_pg_dump_pools();
  check_pg_ls();
  cout << "All tests passed. Success!" << endl;

  shutdown();

  return 0;
}
