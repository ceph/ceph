#include <algorithm>
#include <iostream>
#include <string>
#include <numeric>
#include <regex>
#include <system_error>

#include <boost/process.hpp>
#include <boost/tokenizer.hpp>

namespace bp = boost::process;
using namespace std;

int main(int argc, char** argv)
{
  cout << "Mon Memory Target Test" << endl;

  if (argc != 2) {
    cout << "Syntax: "
         << "ceph_test_mon_memory_target <mon-memory-target-bytes>"
         << endl;
    exit(EINVAL);
  }

  string target_directory("/var/log/ceph/");
  unsigned long maxallowed = stoul(argv[1], nullptr, 10);
  regex reg(R"(cache_size:(\d*)\s)");

  string grep_command("grep _set_new_cache_sizes " + target_directory
                      + "ceph-mon.a.log");
  bp::ipstream is;
  error_code ec;
  bp::child grep(grep_command, bp::std_out > is, ec);
  if (ec) {
    cout << "Error grepping logs! Exiting" << endl;
    cout << "Error: " << ec.value() << " " << ec.message() << endl;
    exit(ec.value());
  }

  string line;
  vector<unsigned long> results;
  while (grep.running() && getline(is, line) && !line.empty()) {
    smatch match;
    if (regex_search(line, match, reg)) {
      results.push_back(stoul(match[1].str()));
    }
  }

  if (results.empty()) {
    cout << "Error: No grep results found!" << endl;
    exit(ENOENT);
  }

  auto maxe = *(max_element(results.begin(), results.end()));
  cout << "Results for mon_memory_target:" << endl;
  cout << "Max: " << maxe << endl;
  cout << "Min: " << *(min_element(results.begin(), results.end())) << endl;
  auto sum = accumulate(results.begin(), results.end(),
                        static_cast<unsigned long long>(0));
  auto mean = sum / results.size();
  cout << "Mean average: " << mean << endl;
  vector<unsigned long> diff(results.size());
  transform(results.begin(), results.end(), diff.begin(),
            [mean](unsigned long x) { return x - mean; });
  auto sump = inner_product(diff.begin(), diff.end(), diff.begin(), 0.0);
  auto stdev = sqrt(sump / results.size());
  cout << "Standard deviation: " << stdev << endl;

  if (maxe > maxallowed) {
    cout << "Error: Mon memory consumption exceeds maximum allowed!" << endl;
    exit(ENOMEM);
  }

  grep.wait();

  cout << "Completed successfully" << endl;
  return 0;
}
