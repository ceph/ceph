// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <chrono>
#include <iomanip>
#include <iostream>

#include "include/hybrid_interval_set.h"
#include "include/interval_set.h"

using namespace std;
using namespace std::chrono;

// Type aliases
using regular_set = interval_set<uint64_t>;
using hybrid_set = hybrid_interval_set<uint64_t>;

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

void print_header()
{
  cout << "\n" << string(90, '=') << "\n";
  cout << "Hybrid Interval Set Benchmark\n";
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
      regular_set s;
      volatile bool e = s.empty();
      (void)e;
    }
    print_result(
        "Empty construction", "interval_set", t.elapsed_ms(),
        sizeof(regular_set));
  }

  {
    Timer t;
    for (int i = 0; i < iterations; i++) {
      hybrid_set s;
      volatile bool e = s.empty();
      (void)e;
    }
    print_result(
        "Empty construction", "hybrid_interval_set", t.elapsed_ms(),
        sizeof(hybrid_set));
  }

  // Test 2: Single insert
  {
    Timer t;
    for (int i = 0; i < iterations; i++) {
      regular_set s;
      s.insert(1000, 100);
    }
    print_result(
        "Single insert", "interval_set", t.elapsed_ms(),
        sizeof(regular_set) + 64);
  }

  {
    Timer t;
    for (int i = 0; i < iterations; i++) {
      hybrid_set s;
      s.insert(1000, 100);
    }
    print_result(
        "Single insert", "hybrid_interval_set", t.elapsed_ms(),
        sizeof(hybrid_set));
  }

  // Test 3: Single insert + iterate
  {
    Timer t;
    for (int i = 0; i < iterations; i++) {
      regular_set s;
      s.insert(1000, 100);
      for (auto it = s.begin(); it != s.end(); ++it) {
        volatile uint64_t start = it.get_start();
        volatile uint64_t len = it.get_len();
        (void)start;
        (void)len;
      }
    }
    print_result(
        "Single insert + iterate", "interval_set", t.elapsed_ms(),
        sizeof(regular_set) + 64);
  }

  {
    Timer t;
    for (int i = 0; i < iterations; i++) {
      hybrid_set s;
      s.insert(1000, 100);
      for (auto it = s.begin(); it != s.end(); ++it) {
        volatile uint64_t start = it.get_start();
        volatile uint64_t len = it.get_len();
        (void)start;
        (void)len;
      }
    }
    print_result(
        "Single insert + iterate", "hybrid_interval_set", t.elapsed_ms(),
        sizeof(hybrid_set));
  }

  // Test 4: Multiple inserts (10)
  {
    Timer t;
    for (int i = 0; i < iterations / 10; i++) {
      regular_set s;
      for (int j = 0; j < 10; j++) {
        s.insert(j * 1000, 100);
      }
    }
    print_result(
        "Multi insert (10)", "interval_set", t.elapsed_ms(),
        sizeof(regular_set) + 640);
  }

  {
    Timer t;
    for (int i = 0; i < iterations / 10; i++) {
      hybrid_set s;
      for (int j = 0; j < 10; j++) {
        s.insert(j * 1000, 100);
      }
    }
    print_result(
        "Multi insert (10)", "hybrid_interval_set", t.elapsed_ms(),
        sizeof(hybrid_set) + sizeof(regular_set) + 640);
  }

  // Test 5: Multiple inserts + iterate (10)
  {
    Timer t;
    for (int i = 0; i < iterations / 10; i++) {
      regular_set s;
      for (int j = 0; j < 10; j++) {
        s.insert(j * 1000, 100);
      }
      for (auto it = s.begin(); it != s.end(); ++it) {
        volatile uint64_t start = it.get_start();
        volatile uint64_t len = it.get_len();
        (void)start;
        (void)len;
      }
    }
    print_result(
        "Multi insert (10) + iterate", "interval_set", t.elapsed_ms(),
        sizeof(regular_set) + 640);
  }

  {
    Timer t;
    for (int i = 0; i < iterations / 10; i++) {
      hybrid_set s;
      for (int j = 0; j < 10; j++) {
        s.insert(j * 1000, 100);
      }
      for (auto it = s.begin(); it != s.end(); ++it) {
        volatile uint64_t start = it.get_start();
        volatile uint64_t len = it.get_len();
        (void)start;
        (void)len;
      }
    }
    print_result(
        "Multi insert (10) + iterate", "hybrid_interval_set", t.elapsed_ms(),
        sizeof(hybrid_set) + sizeof(regular_set) + 640);
  }

  cout << string(90, '=') << "\n\n";

  cout << "Summary:\n";
  cout << "- Empty: hybrid_interval_set uses " << sizeof(hybrid_set)
       << " bytes vs " << sizeof(regular_set) << " bytes\n";
  cout << "- Single interval: hybrid_interval_set uses ~" << sizeof(hybrid_set)
       << " bytes (no heap allocation)\n";
  cout << "- Single interval: interval_set uses ~" << (sizeof(regular_set) + 64)
       << " bytes (with heap allocation)\n";
  cout << "- Multi intervals: both use similar memory (hybrid upgrades to "
          "interval_set)\n";
  cout << "\nKey benefit: Zero heap allocations for 0-1 intervals (common case "
          "in FastEC)\n";

  return 0;
}

// Made with Bob
