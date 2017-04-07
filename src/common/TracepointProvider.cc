// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/TracepointProvider.h"
#include "common/config.h"

TracepointProvider::TracepointProvider(CephContext *cct, const char *library,
                                       const char *config_key)
  : m_cct(cct), m_library(library), m_config_keys{config_key, NULL},
    m_lock("TracepointProvider::m_lock") {
  m_cct->_conf->add_observer(this);
  verify_config(m_cct->_conf);
}

TracepointProvider::~TracepointProvider() {
  m_cct->_conf->remove_observer(this);
  if (m_handle) {
    dlclose(m_handle);
  }
}

void TracepointProvider::handle_conf_change(
    const struct md_config_t *conf, const std::set<std::string> &changed) {
  if (changed.count(m_config_keys[0])) {
    verify_config(conf);
  }
}

void TracepointProvider::verify_config(const struct md_config_t *conf) {
  Mutex::Locker locker(m_lock);
  if (m_handle) {
    return;
  }

  char buf[10];
  char *pbuf = buf;
  if (conf->get_val(m_config_keys[0], &pbuf, sizeof(buf)) != 0 ||
      strncmp(buf, "true", 5) != 0) {
    return;
  }

  m_handle = dlopen(m_library.c_str(), RTLD_NOW | RTLD_NODELETE);
  assert(m_handle);
}

