// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <sys/stat.h>
#include <iostream>
#include <string>
using namespace std;

#include "config.h"
#include "common/ConfUtils.h"

void usage() 
{
  cerr << "usage: cconf [--conf_file filename] [-s] <section> [[-s section] ... ] <key> [default]" << std::endl;
  exit(1);
}

int main(int argc, const char **argv) 
{
  const char *fname = "ceph.conf";
  const char *key = NULL, *defval = NULL;
  char *val;
  int param = 0;
  vector<const char*> args;
  vector<const char *> sections;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  if (args.size() < 2)
    usage();

  for (unsigned i=0; i<args.size(); i++) {
    if (strcmp(args[i],"--conf_file") == 0) {
      if (i < args.size() - 1)
        fname = args[++i];
      else
	usage();
    } else if (strcmp(args[i], "-s") == 0) {
      if (param == 0)
          param++;
      if (i < args.size() - 1)
        sections.push_back(args[++i]);
      else
	usage();
    } else {
      switch (param) {
      	case 0:
	    sections.push_back(args[i]);
	    break;
	case 1:
	    key = args[i];
	    break;
	case 2:
	    defval = args[i];
	    break;
      }
      param++;
    }
  }

  if ((param < 1) || (param > 3))
    usage();

  ConfFile cf(fname);
  cf.parse();

  for (unsigned i=0; i<sections.size(); i++) {
    cf.read(sections[i], key, (char **)&val, NULL);

    if (val) {
      cout << val << std::endl;
      exit(0);
    }
  }

  if (defval) {
    cout << defval << std::endl;
    exit(0);
  }

  exit(1);
}
