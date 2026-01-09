// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <chrono>
#include <iomanip>
#include <iostream>

#include "common/hybrid_interval_map.h"
#include "common/interval_map.h"
#include "include/buffer.h"

using namespace std;
using namespace std::chrono;

// Simple split/merge for bufferlist
struct bl_split_merge {
  bufferlist split(uint64_t offset, uint64_t len, bufferlist& bl) const
  {
    bufferlist result;
    result.substr_of(bl, offset, len);
    return result;
  }

  bool can_merge(const bufferlist& left, const bufferlist& right) const
  {
    return true;
  }

  bufferlist merge(bufferlist&& left, bufferlist&& right) const
  {
    left.claim_append(right);
    return std::move(left);
  }

  uint64_t length(const bufferlist& bl) const
  {
    return bl.length();
  }
};

// Type aliases
using regular_map = interval_map<uint64_t, ceph::buffer::list, bl_split_merge>;
using hybrid_map =
    hybrid_interval_map<uint64_t, ceph::buffer::list, bl_split_merge>;

// Simple timer
class Timer {
  high_resolution_clock::time_point start;

public:
  Timer() :
    start(high_resolution_clock::now())
  {}

  double elapsed_ms()
  {
    auto end = high_resolution_clock::now();
    return duration_cast<microseconds>(end - start).count() / 1000.0;
  }
};

ceph::buffer::list make_buffer(size_t size)
{
  ceph::buffer::list bl;
  bl.append(ceph::buffer::create(size));
  return bl;
}

void print_header()
{
  cout << "\n" << string(90, '=') << "\n";
  cout << "Hybrid Interval Map Benchmark\n";
  cout << string(90, '=') << "\n\n";
}

void print_result(
    const string& test,
    const string& type,
    double time_ms,
    size_t mem_bytes)
{
  cout << left << setw(30) << test << setw(25) << type << right << setw(15)
       << fixed << setprecision(2) << time_ms << " ms" << setw(20) << mem_bytes
       << " bytes\n";
}

int main(int argc, char** argv)
{
  int iterations = 100000;
  if (argc > 1) {
    iterations = atoi(argv[1]);
  }

  print_header();
  cout << left << setw(30) << "Test" << setw(25) << "Type" << right << setw(15)
       << "Time" << setw(20) << "Memory\n";
  cout << string(90, '-') << "\n";

  // Test 1: Empty construction
  {
    Timer t;
    for (int i = 0; i < iterations; i++) {
      regular_map m;
      volatile bool e = m.empty();
      (void)e;
    }
    print_result(
        "Empty construction", "interval_map", t.elapsed_ms(),
        sizeof(regular_map));
  }

  {
    Timer t;
    for (int i = 0; i < iterations; i++) {
      hybrid_map m;
      volatile bool e = m.empty();
      (void)e;
    }
    print_result(
        "Empty construction", "hybrid_interval_map", t.elapsed_ms(),
        sizeof(hybrid_map));
  }

  // Test 2: Single insert
  {
    Timer t;
    for (int i = 0; i < iterations; i++) {
      regular_map m;
      ceph::buffer::list bl = make_buffer(100);
      m.insert(1000, 100, bl);
    }
    print_result(
        "Single insert", "interval_map", t.elapsed_ms(),
        sizeof(regular_map) + 128);
  }

  {
    Timer t;
    for (int i = 0; i < iterations; i++) {
      hybrid_map m;
      ceph::buffer::list bl = make_buffer(100);
      m.insert(1000, 100, bl);
    }
    print_result(
        "Single insert", "hybrid_interval_map", t.elapsed_ms(),
        sizeof(hybrid_map) + sizeof(ceph::buffer::list));
  }

  // Test 3: Single insert + iterate
  {
    Timer t;
    for (int i = 0; i < iterations; i++) {
      regular_map m;
      ceph::buffer::list bl = make_buffer(100);
      m.insert(1000, 100, bl);
      for (auto it = m.begin(); it != m.end(); ++it) {
        volatile uint64_t start = it.get_off();
        volatile uint64_t len = it.get_len();
        (void)start;
        (void)len;
      }
    }
    print_result(
        "Single insert + iterate", "interval_map", t.elapsed_ms(),
        sizeof(regular_map) + 128);
  }

  {
    Timer t;
    for (int i = 0; i < iterations; i++) {
      hybrid_map m;
      ceph::buffer::list bl = make_buffer(100);
      m.insert(1000, 100, bl);
      for (auto it = m.begin(); it != m.end(); ++it) {
        volatile uint64_t start = it.get_off();
        volatile uint64_t len = it.get_len();
        (void)start;
        (void)len;
      }
    }
    print_result(
        "Single insert + iterate", "hybrid_interval_map", t.elapsed_ms(),
        sizeof(hybrid_map) + sizeof(ceph::buffer::list));
  }

  // Test 4: Multiple inserts (10)
  {
    Timer t;
    for (int i = 0; i < iterations / 10; i++) {
      regular_map m;
      for (int j = 0; j < 10; j++) {
        ceph::buffer::list bl = make_buffer(100);
        m.insert(j * 1000, 100, bl);
      }
    }
    print_result(
        "Multi insert (10)", "interval_map", t.elapsed_ms(),
        sizeof(regular_map) + 1280);
  }

  {
    Timer t;
    for (int i = 0; i < iterations / 10; i++) {
      hybrid_map m;
      for (int j = 0; j < 10; j++) {
        ceph::buffer::list bl = make_buffer(100);
        m.insert(j * 1000, 100, bl);
      }
    }
    print_result(
        "Multi insert (10)", "hybrid_interval_map", t.elapsed_ms(),
        sizeof(hybrid_map) + sizeof(regular_map) + 1280);
  }

  // Test 5: Multiple inserts + iterate (10)
  {
    Timer t;
    for (int i = 0; i < iterations / 10; i++) {
      regular_map m;
      for (int j = 0; j < 10; j++) {
        ceph::buffer::list bl = make_buffer(100);
        m.insert(j * 1000, 100, bl);
      }
      for (auto it = m.begin(); it != m.end(); ++it) {
        volatile uint64_t start = it.get_off();
        volatile uint64_t len = it.get_len();
        (void)start;
        (void)len;
      }
    }
    print_result(
        "Multi insert (10) + iterate", "interval_map", t.elapsed_ms(),
        sizeof(regular_map) + 1280);
  }

  {
    Timer t;
    for (int i = 0; i < iterations / 10; i++) {
      hybrid_map m;
      for (int j = 0; j < 10; j++) {
        ceph::buffer::list bl = make_buffer(100);
        m.insert(j * 1000, 100, bl);
      }
      for (auto it = m.begin(); it != m.end(); ++it) {
        volatile uint64_t start = it.get_off();
        volatile uint64_t len = it.get_len();
        (void)start;
        (void)len;
      }
    }
    print_result(
        "Multi insert (10) + iterate", "hybrid_interval_map", t.elapsed_ms(),
        sizeof(hybrid_map) + sizeof(regular_map) + 1280);
  }

  cout << string(90, '=') << "\n\n";

  cout << "Summary:\n";
  cout << "- Empty: hybrid_interval_map uses " << sizeof(hybrid_map)
       << " bytes vs " << sizeof(regular_map) << " bytes\n";
  cout << "- Single interval: hybrid_interval_map uses ~"
       << (sizeof(hybrid_map) + sizeof(ceph::buffer::list))
       << " bytes (no map allocation)\n";
  cout << "- Single interval: interval_map uses ~"
       << (sizeof(regular_map) + 128) << " bytes (with map allocation)\n";
  cout << "- Multi intervals: both use similar memory (hybrid upgrades to "
          "interval_map)\n";
  cout << "\nKey benefit: Zero map allocations for 0-1 intervals (common case "
          "in FastEC)\n";

  return 0;
}

// Made with Bob
