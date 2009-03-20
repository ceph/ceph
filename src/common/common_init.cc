
#include "config.h"
#include "tls.h"

void common_init(std::vector<const char*>& args, const char *module_type, bool daemon)
{
  tls_init();
  tls_get_val()->disable_assert = 0;

  parse_startup_config_options(args, module_type);

  if (daemon) {
    cout << " ** WARNING: Ceph is still under heavy development, and is only suitable for **\n";
    cout << " **          testing and review.  Do not trust it with important data.       **" << std::endl;
    
    g_conf.daemonize = true;
    g_conf.log_to_stdout = false;
  } else {
    g_conf.daemonize = false;
    g_conf.log_to_stdout = true;
    g_conf.logger = false;
  }

  parse_config_options(args);

  // open log file?
  if (!g_conf.log_to_stdout)
    _dout_open_log();
}

