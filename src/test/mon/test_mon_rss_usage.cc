#include <algorithm>
#include <iostream>
#include <fstream>
#include <string>
#include <numeric>
#include <regex>
#include <cmath>
#include <system_error>

using namespace std;

int main(int argc, char **argv)
{
  cout << "Mon RSS Usage Test" << endl;

  if (argc != 2) {
    cout << "Syntax: "
         << "ceph_test_mon_rss_usage <mon-memory-target-bytes>"
         << endl;
    exit(EINVAL);
  }

  unsigned long maxallowed = stoul(argv[1], nullptr, 10);
  // Set max allowed RSS usage to be 125% of mon-memory-target
  maxallowed *= 1.25;

  string target_directory("/var/log/ceph/");
  string filePath = target_directory + "ceph-mon-rss-usage.log";
  ifstream buffer(filePath.c_str());
  string line;
  vector<unsigned long> results;
  while(getline(buffer, line) && !line.empty()) {
    string rssUsage;
    size_t pos = line.find(':');
    if (pos != string::npos) {
      rssUsage = line.substr(0, pos);
    }
    if (!rssUsage.empty()) {
      results.push_back(stoul(rssUsage));
    }
  }

  buffer.close();
  if (results.empty()) {
    cout << "Error: No grep results found!" << endl;
    exit(ENOENT);
  }

  auto maxe = *(max_element(results.begin(), results.end()));
  cout << "Stats for mon RSS Memory Usage:" << endl;
  cout << "Parsed " << results.size() << " entries." << endl;
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
  cout << fixed <<  "Standard deviation: " << stdev << endl;

  if (maxe > maxallowed) {
    cout << "Error: Mon RSS memory usage exceeds maximum allowed!" << endl;
    exit(ENOMEM);
  }

  cout << "Completed successfully" << endl;
  return 0;
}
