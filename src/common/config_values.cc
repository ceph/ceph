// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "config_values.h"

#include "config.h"

ConfigValues::set_value_result_t
ConfigValues::set_value(const std::string_view key,
                        Option::value_t&& new_value,
                        int level)
{  
  if (auto p = values.find(key); p != values.end()) {
    auto q = p->second.find(level);
    if (q != p->second.end()) {
      if (new_value == q->second) {
        return SET_NO_CHANGE;
      }
      q->second = std::move(new_value);
    } else {
      p->second[level] = std::move(new_value);
    }
    if (p->second.rbegin()->first > level) {
      // there was a higher priority value; no effect
      return SET_NO_EFFECT;
    } else {
      return SET_HAVE_EFFECT;
    }
  } else {
    values[key][level] = std::move(new_value);
    return SET_HAVE_EFFECT;
  }
}

int ConfigValues::rm_val(const std::string_view key, int level)
{
  auto i = values.find(key);
  if (i == values.end()) {
    return -ENOENT;
  }
  auto j = i->second.find(level);
  if (j == i->second.end()) {
    return -ENOENT;
  }
  bool matters = (j->first == i->second.rbegin()->first);
  i->second.erase(j);
  if (matters) {
    return SET_HAVE_EFFECT;
  } else {
    return SET_NO_EFFECT;
  }
}

std::pair<Option::value_t, bool>
ConfigValues::get_value(const std::string_view name, int level) const
{
  auto p = values.find(name);
  if (p != values.end() && !p->second.empty()) {
    // use highest-priority value available (see CONF_*)
    if (level < 0) {
      return {p->second.rbegin()->second, true};
    } else if (auto found = p->second.find(level);
               found != p->second.end()) {
      return {found->second, true};
    }
  }
  return {Option::value_t{}, false};
}

void ConfigValues::set_logging(int which, const char* val)
{
  int log, gather;
  int r = sscanf(val, "%d/%d", &log, &gather);
  if (r >= 1) {
    if (r < 2) {
      gather = log;
    }
    subsys.set_log_level(which, log);
    subsys.set_gather_level(which, gather);
  }
}

bool ConfigValues::contains(const std::string_view key) const
{
  return values.count(key);
}
