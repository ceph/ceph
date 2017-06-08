
#include "SubsystemMap.h"

namespace ceph {
namespace log {

void SubsystemMap::add(unsigned subsys, std::string name, int log, int gather)
{
  if (subsys >= m_subsys.size())
    m_subsys.resize(subsys + 1);
  m_subsys[subsys].name = name;
  m_subsys[subsys].log_level = log;
  m_subsys[subsys].gather_level = gather;
  if (name.length() > m_max_name_len)
    m_max_name_len = name.length();
}

void SubsystemMap::set_log_level(unsigned subsys, int log)
{
  assert(subsys < m_subsys.size());
  m_subsys[subsys].log_level = log;
}

void SubsystemMap::set_gather_level(unsigned subsys, int gather)
{
  assert(subsys < m_subsys.size());
  m_subsys[subsys].gather_level = gather;
}

}
}
