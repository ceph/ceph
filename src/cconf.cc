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
  cerr << "usage: cconf [--conf_file filename] <section> <key> [defval]" << std::endl;
  exit(1);
}

int main(int argc, const char **argv) 
{
  const char *fname = "ceph.conf", *section = NULL;
  const char *key = NULL, *defval = NULL;
  char *val;
  int param = 0;
  vector<const char*> args;
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
    } else {
      switch (param) {
      	case 0:
	    section = args[i];
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
  cf.read(section, key, (char **)&val, defval);

  if (val)
    cout << val << std::endl;
  else
    exit(1);

  exit(0);
}
