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


const char *id = NULL, *type = NULL;
char *name, *alt_name;

#define MAX_VAR_LEN 32


static bool get_var(const char *str, int pos, char *var_name, int len, int *new_pos)
{
  int bracket = (str[pos] == '{');
  int out_pos = 0;

  if (bracket) {
    pos++;
  }

  while (str[pos] &&
	((bracket && str[pos] != '}') ||
	 isalnum(str[pos]))) {
	var_name[out_pos] = str[pos];
	
	out_pos	++;
	if (out_pos == len)
		return false;
	pos++;
  }

  var_name[out_pos] = '\0';

  if (bracket && (str[pos] == '}'))
	pos++;

  *new_pos = pos;

  return true;
}

static const char *var_val(char *var_name)
{
	if (strcmp(var_name, "type")==0)
		return type;
	if (strcmp(var_name, "id")==0)
		return id;
	if (strcmp(var_name, "num")==0)
		return id;
	if (strcmp(var_name, "name")==0)
		return name;

	return "";
}

#define MAX_LINE 256

static char *post_process_val(const char *val)
{
  char var_name[MAX_VAR_LEN];
  char buf[MAX_LINE];
  int i=0;
  int out_pos = 0;

  while (val[i] && (out_pos < MAX_LINE - 1)) {
    if (val[i] == '$') {
	if (get_var(val, i+1, var_name, MAX_VAR_LEN, &i)) {
		out_pos += snprintf(buf+out_pos, MAX_LINE-out_pos, "%s", var_val(var_name));
	} else {
	  ++i;
	}
    } else {
	buf[out_pos] = val[i];
    	++out_pos;
    	++i;
    }
  }

  buf[out_pos] = '\0';

  return strdup(buf);
}

static void usage() 
{
  cerr << "usage: cconf <-c filename> [-t type] [-i id] [-l|--list_sections <prefix>] [-s <section>] [[-s section] ... ] <key> [default]" << std::endl;
  exit(1);
}

int main(int argc, const char **argv) 
{
  const char *fname = NULL;
  const char *key = NULL, *defval = NULL;
  const char *list_sections = 0;
  char *val;
  int param = 0;
  deque<const char*> args;
  deque<const char *> sections;
  argv_to_deq(argc, argv, args);
  env_to_deq(args);

  if (args.size() < 2)
    usage();

  for (unsigned i=0; i<args.size(); i++) {
    if (strcmp(args[i], "-c") == 0) {
      if (i < args.size() - 1)
        fname = args[++i];
      else
	usage();
    } else if (strcmp(args[i], "-t") == 0) {
      if (param == 0)
          param++;
      if (i < args.size() - 1)
        type = args[++i];
      else
	usage();
    } else if (strcmp(args[i], "-i") == 0) {
      if (i < args.size() - 1)
        id = args[++i];
      else
	usage();
    } else if (strcmp(args[i], "-l") == 0 ||
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

  if (!list_sections && (param < 1 || param > 3))
    usage();

  if (!fname)
    usage();

  ConfFile cf(fname);
  parse_config_file(&cf, true, type, id);

  if (list_sections) {
    for (std::list<ConfSection*>::const_iterator p = cf.get_section_list().begin();
	 p != cf.get_section_list().end();
	 p++) {
      if (strncmp(list_sections, (*p)->get_name().c_str(), strlen(list_sections)) == 0)
	cout << (*p)->get_name() << std::endl;
    }
    return 0;
  }

  if (id) {
       name = (char *)malloc(strlen(type) + strlen(id) + 2);
       sprintf(name, "%s.%s", type, id);
       alt_name = (char *)malloc(strlen(type) + strlen(id) + 1);
       sprintf(alt_name, "%s%s", type, id);
  } else {
       name = (char *)type;
  }

  if (type)
    sections.push_front(type);

  if (alt_name)
    sections.push_front(alt_name);
  if (name)
    sections.push_front(name);

  sections.push_back("global");

  for (unsigned i=0; i<sections.size(); i++) {
    cf.read(sections[i], key, (char **)&val, NULL);

    if (val) {
      cout << post_process_val(val) << std::endl;
      exit(0);
    }
  }

  if (defval) {
    cout << post_process_val(defval) << std::endl;
    exit(0);
  }

  exit(1);
}
