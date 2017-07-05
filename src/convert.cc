
#include <iostream>
#include "include/acconfig.h"
using namespace std;
#define _STR(x) #x
#define STRINGIFY(x) _STR(x)

#define OPTION_OPT_LONGLONG(name, def_val) name((1LL) * def_val),
#define OPTION_OPT_STR(name, def_val) name(def_val),
#define OPTION_OPT_DOUBLE(name, def_val) name(def_val),
#define OPTION_OPT_FLOAT(name, def_val) name(def_val),
#define OPTION_OPT_BOOL(name, def_val) name(def_val),
#define OPTION_OPT_ADDR(name, def_val) name(def_val),
#define OPTION_OPT_U32(name, def_val) name(def_val),
#define OPTION_OPT_U64(name, def_val) name(((uint64_t)1) * def_val),
#define OPTION_OPT_UUID(name, def_val) name(def_val),

string convert_type(string t) {
  if (t == "OPT_STR")
    return "TYPE_STR";
  if (t == "OPT_DOUBLE" || t == "OPT_FLOAT")
    return "TYPE_FLOAT";
  if (t == "OPT_BOOL")
    return "TYPE_BOOL";
  if (t == "OPT_U32" ||
      t == "OPT_U64" ||
      t == "OPT_INT" ||
      t == "OPT_LONGLONG")
    return "TYPE_INT";
  if (t == "OPT_UUID")
    return "TYPE_UUID";
  if (t == "OPT_ADDR")
    return "TYPE_ADDR";
  return "ERROR";
}

#define OPTION(name, type, def_val)					\
  cout << "  Option(\"" << STRINGIFY(name) << "\", Option::" << convert_type(STRINGIFY(type)) \
  << ", Option::LEVEL_ADVANCED)\n"					\
  << "  .set_default(" << STRINGIFY(def_val) << ")\n"			\
  << "  .set_description(\"\"),\n\n";
  
#define OPTION_VALIDATOR(name)
#define SAFE_OPTION(name, type, def_val)            \
  cout << "  Option(\"" << STRINGIFY(name) << "\", Option::" << convert_type(STRINGIFY(type)) \
  << ", Option::LEVEL_ADVANCED)\n"					\
  << "  .set_default(" << STRINGIFY(def_val) << ")\n"			\
  << "  .set_description(\"\")\n";                  \
  << "  .set_safe(),\n\n";
  
#define SUBSYS(name, log, gather)
#define DEFAULT_SUBSYS(log, gather)

int main(int argc, char **argv)
{
  #include "common/legacy_config_opts.h"
}
