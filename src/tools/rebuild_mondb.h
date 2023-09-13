#pragma once
#include <string>

class ObjectStore;
class OSDSuperblock;

int update_mon_db(ObjectStore& fs, OSDSuperblock& sb,
                  const std::string& keyring_path,
                  const std::string& store_path);
