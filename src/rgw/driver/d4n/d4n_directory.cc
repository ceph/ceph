#include <algorithm>
#include <boost/asio/consign.hpp>
#include <boost/algorithm/string.hpp>
#include <memory>
#include "common/async/blocked_completion.h"
#include "common/dout.h" 
#include "d4n_directory.h"

namespace rgw::d4n {

std::string ObjectDirectory::build_index(const std::string& bucket_id, const std::string& obj_name) 
{
  return bucket_id + "_" + obj_name;
}

std::string BlockDirectory::build_index(CacheBlock* block) 
{
  return block->cacheObj.bucketName + "_" + block->cacheObj.objName + "_" + std::to_string(block->blockID) + "_" + std::to_string(block->size);
}


} // namespace rgw::d4n
