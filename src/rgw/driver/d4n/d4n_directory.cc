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
  return url_encode(bucket_id, true) + "#" + url_encode(obj_name, true);
}

std::string BlockDirectory::build_index(const CacheBlock* block) 
{
  std::string key = url_encode(block->cacheObj.bucketName, true) + "#" + url_encode(block->cacheObj.objName, true) + "/block/";
  if (block->size > 0) {
    key += block->version + "/";
  }
  key += std::to_string(block->blockID) + "/" + std::to_string(block->size);
  return key;
}


} // namespace rgw::d4n
