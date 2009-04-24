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
  char *section;
  int param = 0;
  vector<const char*> args, nargs;
  deque<const char *> sections;
  unsigned i;
  DEFINE_CONF_VARS(usage);

  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  if (args.size() < 2)
    usage();

  FOR_EACH_ARG(args) {
    if (CONF_ARG_EQ("type", 't')) {
      CONF_SAFE_SET_ARG_VAL(&type, OPT_STR);
    } else {
      nargs.push_back(args[i]);
    }
  }
  args.swap(nargs);

  common_init(args, type, false);

  FOR_EACH_ARG(args) {
    if (CONF_ARG_EQ("list_sections", 'l')) {
    	CONF_SAFE_SET_ARG_VAL(&list_sections, OPT_STR);
    } else if (CONF_ARG_EQ("section", 's')) {
    	CONF_SAFE_SET_ARG_VAL(&section, OPT_STR);
        sections.push_back(section);
    } else if (*args[i] != '-') {
      switch (param) {
	case 0:
	    key = args[i];
	    break;
	case 1:
	    defval = args[i];
	    break;
      }
      param++;
    } else {
      cerr << "unrecognized argument: " << args[i] << std::endl;
      usage();
    }
  }

  if (!list_sections && (param < 1 || param > 2))
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
      free(val);
      exit(0);

}
