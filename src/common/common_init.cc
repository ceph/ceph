
#include "config.h"
#include "tls.h"

#include "include/color.h"

#include "auth/KeyRing.h"
#include "auth/AuthSupported.h"

void common_set_defaults(bool daemon)
{
  if (daemon) {
    cout << TEXT_YELLOW << " ** WARNING: Ceph is still under heavy development, and is only suitable for **" << TEXT_NORMAL << std::endl;
    cout << TEXT_YELLOW <<  " **          testing and review.  Do not trust it with important data.       **" << TEXT_NORMAL << std::endl;

    g_conf.daemonize = true;
    g_conf.logger = true;
    g_conf.log_to_stdout = false;
  } else {
    g_conf.pid_file = 0;
  }
}

void common_init(std::vector<const char*>& args, const char *module_type, bool init_keys)
{
  tls_init();
  tls_get_val()->disable_assert = 0;

  parse_startup_config_options(args, module_type);
  parse_config_options(args);

  if (g_conf.log_file && g_conf.log_file[0])
    g_conf.log_to_stdout = false;

  // open log file?
  if (!g_conf.log_to_stdout)
    _dout_open_log();

  if (init_keys && is_supported_auth(CEPH_AUTH_CEPHX)) {
    g_keyring.load(g_conf.keyring);

    if (strlen(g_conf.key)) {
      string k = g_conf.key;
      EntityAuth ea;
      ea.key.decode_base64(k);
      g_keyring.add(*g_conf.entity_name, ea);
    } else if (strlen(g_conf.keyfile)) {
      char buf[100];
      int fd = ::open(g_conf.keyfile, O_RDONLY);
      if (fd < 0) {
	cerr << "unable to open " << g_conf.keyfile << ": " << strerror(errno) << std::endl;
	exit(1);
      }
      int len = ::read(fd, buf, sizeof(buf));
      if (len < 0) {
	cerr << "unable to read key from " << g_conf.keyfile << ": " << strerror(errno) << std::endl;
	exit(1);
      }
      ::close(fd);

      buf[len] = 0;
      string k = buf;
      EntityAuth ea;
      ea.key.decode_base64(k);
      g_keyring.add(*g_conf.entity_name, ea);
    }
  }
}

