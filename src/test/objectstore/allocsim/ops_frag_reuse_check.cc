#include "ops_parser.h"
#include <vector>
#include <string>
#include <iostream>
#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

using namespace std;
using namespace ceph;
namespace po = boost::program_options;

void usage(po::options_description &desc) {
  cout << desc << std::endl;
}

int op_comparison_by_object_date(const Op& lhs, const Op& rhs) {
  if (*lhs.object < *rhs.object) return true;
  if (*lhs.object > *rhs.object) return false;
  return lhs.at < rhs.at;
}

void fragment_check(const vector<Op>& ops)
{
  shared_ptr<string> last(make_shared<string>(""));
  std::map<uint32_t, uint32_t> plaster;
  auto cut = [&](uint32_t o, uint32_t l) {
    auto it = plaster.lower_bound(o);
    if (it != plaster.begin()) {
      --it;
      if (it->first + it->second > o) {
        it->second = o - it->first;
      }
      ++it;
    }

    while(it != plaster.end() && it->first < o + l) {
      if (it->first + it->second <= o + l) {
        it = plaster.erase(it);
      } else {
        uint32_t end = it->first + it->second;
        plaster.erase(it);
        plaster.emplace(o + l, end - (o + l));
        assert(end - (o + l) > 0);
        break;
      }
    }
  };
  auto print = [&]() {
    std::cout << std::hex;
    for (const auto& x : plaster) {
      std::cout << x.first << "~" << x.second << " ";
    }
    std::cout << std::dec << std::endl;
  };
  //reads
  uint32_t exact = 0;
  uint32_t sub = 0;
  uint32_t to_empty = 0;
  uint32_t crossed = 0;
  uint32_t size4096 = 0;
  //writes
  uint32_t writes = 0;

  auto read_check = [&](uint32_t o, uint32_t l) {
    auto it = plaster.lower_bound(o);
    if (it != plaster.begin()) {
      --it;
      if (it->first + it->second < o) {
        ++it;
      }
    }
    uint32_t c = 0;
    while(it != plaster.end() && it->first < o + l) {
      if (it->first == o && it->second == l) {
        exact++;
        break;
      }
      if (it->first < o && o + l < it->first + it->second) {
        sub++;
        break;
      }
      c++;
      ++it;
    }
    if (c > 0) {
      if (c == 1) to_empty++;
      else crossed++;
    }
  };
  for (const auto& x : ops) {
    if (*x.object != *last) {
      // new era
      last = x.object;
      plaster.clear();
      std::cout << "ex=" << exact << " sub=" << sub << " toe=" << to_empty
      << " crx=" << crossed << " 4k=" << size4096 << " wri=" << writes << std::endl;
      exact =  sub = to_empty = crossed = size4096 = writes = 0;
    }
    if (0)
      std::cout << *x.object << " " << x.at << " " << (int)x.type << " "
      << std::hex << x.offset << "~" << x.length << std::dec << std::endl;
    switch (x.type) {
      case Read:
        if (x.length == 4096) {
          size4096++;
          break;
        }
        read_check(x.offset, x.length);
        break;
      case Truncate:
        writes++;
        cut(x.offset, 100000000);
        break;
      case WriteFull:
        writes++;
        plaster.clear();
        plaster.emplace(x.offset, x.length);
        break;
      case Zero:
      case Write:
        writes++;
        cut(x.offset, x.length);
        plaster.emplace(x.offset, x.length);
        //print();
        break;
    }
    if (0)
      std::cout << "ex=" << exact << " sub=" << sub << " toe=" << to_empty
      << " crx=" << crossed << " 4k=" << size4096 << std::endl;
  }
}



int main(int argc, char** argv) {
  vector<Op> ops;
  uint64_t max_buffer_size = 0; // We can use a single buffer for writes and trim it at will. The buffer will be the size of the maximum length op.

  // options
  uint64_t nparser_threads = 16;
  string file("input.txt");
  po::options_description po_options("Options");
  po_options.add_options()
    ("help,h", "produce help message")
    ("input-files,i", po::value<vector<string>>()->multitoken(), "List of input files (output of op_scraper.py). Multiple files will be merged and sorted by time order")
    ("parser-threads", po::value<uint64_t>(&nparser_threads)->default_value(16), "Number of parser threads")
    ;

  po::options_description po_all("All options");
  po_all.add(po_options);

  po::variables_map vm;
  po::parsed_options parsed = po::command_line_parser(argc, argv).options(po_all).allow_unregistered().run();
  po::store( parsed, vm);
  po::notify(vm);
  
  if (vm.count("help")) {
    usage(po_all);
    exit(EXIT_SUCCESS);
  }

  vector<string> input_files = vm["input-files"].as<vector<string>>();
  parse_files(input_files, nparser_threads, ops, max_buffer_size);

  cout << "Sorting ops by object / date..." << endl;
  sort(ops.begin(), ops.end(), op_comparison_by_object_date);
  cout << "Sorting ops by object / date done" << endl;



  fragment_check(ops);
}