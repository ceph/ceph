#include <sys/types.h>
#include <cstdint>
#include <dirent.h>
#include <errno.h>
#include <vector>
#include <string>
#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

using namespace std;

int getPidByName(string procName)
{
  int pid = -1;

  // Open the /proc directory
  DIR *dp = opendir("/proc");
  if (dp != NULL)
  {
    // Enumerate all entries in '/proc' until process is found
    struct dirent *dirp;
    while (pid < 0 && (dirp = readdir(dp)))
    {
      // Skip non-numeric entries
      int id = atoi(dirp->d_name);
      if (id > 0)
      {
        // Read contents of virtual /proc/{pid}/cmdline file
        string cmdPath = string("/proc/") + dirp->d_name + "/cmdline";
        ifstream cmdFile(cmdPath.c_str());
        string cmdLine;
        getline(cmdFile, cmdLine);
        if (!cmdLine.empty())
        {
          // Keep first cmdline item which contains the program path
          size_t pos = cmdLine.find('\0');
          if (pos != string::npos) {
            cmdLine = cmdLine.substr(0, pos);
          }
          // Get program name only, removing the path
          pos = cmdLine.rfind('/');
          if (pos != string::npos) {
            cmdLine = cmdLine.substr(pos + 1);
          }
          // Compare against requested process name
          if (procName == cmdLine) {
            pid = id;
          }
        }
      }
    }
  }

  closedir(dp);

  return pid;
}

uint64_t getRssUsage(string pid)
{
  int totalSize = 0;
  int resSize = 0;

  string statmPath = string("/proc/") + pid + "/statm";
  ifstream buffer(statmPath);
  buffer >> totalSize >> resSize;
  buffer.close();

  long page_size = sysconf(_SC_PAGE_SIZE);
  uint64_t rss = resSize * page_size;

  return rss;
}

int main(int argc, char* argv[])
{
  if (argc != 2) {
    cout << "Syntax: "
         << "ceph_test_log_rss_usage <process name>"
         << endl;
    exit(EINVAL);
  }
  uint64_t rss = 0;
  int pid = getPidByName(argv[1]);
  string rssUsage;

  // Use the pid to get RSS memory usage
  // and print it to stdout
  if (pid != -1) {
    rss = getRssUsage(to_string(pid));
  } else {
    cout << "Process " << argv[1] << " NOT FOUND!\n" << endl;
    exit(ESRCH);
  }

  rssUsage = to_string(rss) + ":" + to_string(pid) + ":";
  cout << rssUsage.c_str() << endl;
  return 0;
}
