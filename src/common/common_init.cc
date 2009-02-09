
#include "config.h"
#include "tls.h"

void common_init(std::vector<const char*>& args, bool open)
{
  parse_config_options(args, open);
  tls_init();
}

