#include "rgw_exporter.h"
#include "common/ceph_context.h"
#include <filesystem>

int RGWExporter::start(const DoutPrefixProvider* dpp, rgw::sal::Driver* driver)
{
  auto cct = dpp->get_cct();
  std::string path = cct->_conf.get_val<std::string>("rgw_data") + "/usage_cache";
  std::filesystem::create_directories(path);
  cache = std::make_unique<RGWUsageCache>(path);
  manager = std::make_unique<RGWUsageManager>(dpp, driver, cache.get());
  manager->start();
  return 0;
}

void RGWExporter::stop()
{
  if (manager) {
    manager->stop();
    manager.reset();
  }
  cache.reset();
}

