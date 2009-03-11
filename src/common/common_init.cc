
#include "config.h"
#include "tls.h"

void common_init(std::vector<const char*>& args, bool open)
{
  tls_init();
  tls_get_val()->disable_assert = 0;
  parse_startup_config_options(args);
  parse_config_options(args);

  // open log file?
  if (open)
    _dout_open_log();
}

