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
#include "common/common_init.h"

const char *id = NULL, *type = NULL;
char *name, *alt_name;

static void usage() 
{
  cerr << "usage: cconf <-c filename> [-t type] [-i id] [-l|--list_sections <prefix>] [-s <section>] [[-s section] ... ] <key> [default]" << std::endl;
  exit(1);
}

int main(int argc, const char **argv) 
{
  const char *key = NULL, *defval = NULL;
  const char *list_sections = 0;
  char *val;
  int param = 0;
  vector<const char*> args, nargs;
  deque<const char *> sections;
  unsigned i;

  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  if (args.size() < 2)
    usage();

  for (unsigned i=0; i<args.size(); i++) {
    if (strcmp(args[i], "-t") == 0) {
      if (i < args.size() - 1)
        type = args[++i];
      else
	usage();
    } else {
	nargs.push_back(args[i]);
    }
  }
  args.swap(nargs);

  common_init(args, type, false);

  for (unsigned i=0; i<args.size(); i++) {
      if (strcmp(args[i], "-l") == 0 ||
	       strcmp(args[i], "--list_sections") == 0) {
      if (i < args.size() - 1)
	list_sections = args[++i];
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
	    key = args[i];
	    break;
	case 1:
	    defval = args[i];
	    break;
      }
      param++;
    }
  }

  if (!list_sections && (param < 1 || param > 3))
    usage();

  ConfFile *cf = conf_get_conf_file();

  assert(cf);

  if (list_sections) {
    for (std::list<ConfSection*>::const_iterator p = cf->get_section_list().begin();
	 p != cf->get_section_list().end();
	 p++) {
      if (strncmp(list_sections, (*p)->get_name().c_str(), strlen(list_sections)) == 0)
	cout << (*p)->get_name() << std::endl;
    }
    return 0;
  }

  conf_read_key(NULL, key, OPT_STR, (char **)&val, NULL);

  if (val)
    goto done_ok;

  for (i=0; i<sections.size(); i++) {
    cf->read(sections[i], key, (char **)&val, NULL);

    if (val)
	goto done_ok;
  }

  if (defval) {
    val = conf_post_process_val(defval);
    goto done_ok;
  }

  exit(1);

done_ok:
      cout << val << std::endl;
      exit(0);

}
