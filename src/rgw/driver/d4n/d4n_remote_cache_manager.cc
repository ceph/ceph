#include "d4n_remote_cache_manager.h"

#include "../../../common/async/yield_context.h"
#include "common/async/blocked_completion.h"

namespace rgw { namespace d4n {

int RemoteCacheGetOp::send_request(const DoutPrefixProvider* dpp, optional_yield& y, bufferlist* bl)
{
  in_bl.clear();
  cb = std::make_unique<RemoteGetCB>(&in_bl);
  RGWAccessKey accessKey;
  std::string findKey;

  std::unique_ptr<rgw::sal::User> c_user = driver->get_user(op.bucket_owner);
  int ret = c_user->load_user(dpp, y);
  if (ret < 0) {
    return -EPERM;
  }

  if (c_user->get_info().access_keys.empty()) {
    return -EINVAL;
  }

  accessKey.id = c_user->get_info().access_keys.begin()->second.id;
  accessKey.key = c_user->get_info().access_keys.begin()->second.key;

  HostStyle host_style = PathStyle;
  std::map<std::string, std::string> extra_headers;
  uint64_t end_offset = op.offset + op.len - 1;
  std::string range_val = "bytes=" + std::to_string(op.offset) + "-" + std::to_string(end_offset);
  extra_headers["RANGE"] = std::move(range_val);

  auto resource = get_resource(op.bucket_name, op.oid);
  sender = std::make_unique<RGWRESTStreamRWRequest>(dpp->get_cct(), "GET", op.remote_addr, cb.get(), nullptr, nullptr, "", host_style);

  ret = sender->send_request(dpp, &accessKey, extra_headers, resource, nullptr, bl);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

rgw::AioResultList RemoteCacheGetOp::send_request(const DoutPrefixProvider* dpp, rgw::Aio* aio, uint64_t cost, uint64_t id, optional_yield& y)
{
  in_bl.clear();
  cb = std::make_unique<RemoteGetCB>(&in_bl);
  this->aio = aio;

  auto op_func = [this, dpp, y](Aio* aio, AioResult& r) mutable {
    this->r = &r;
    RGWAccessKey accessKey;
    std::string findKey;

    std::unique_ptr<rgw::sal::User> c_user = driver->get_user(op.bucket_owner);
    int ret = c_user->load_user(dpp, y);
    if (ret < 0) {
      r.result = -EPERM;
      aio->put(r);
      this->r = nullptr;
    }

    if (c_user->get_info().access_keys.empty()) {
      r.result = -EINVAL;
      aio->put(r);
      this->r = nullptr;
    }

    accessKey.id = c_user->get_info().access_keys.begin()->second.id;
    accessKey.key = c_user->get_info().access_keys.begin()->second.key;

    HostStyle host_style = PathStyle;
    std::map<std::string, std::string> extra_headers;
    uint64_t end_offset = op.offset + op.len - 1;
    std::string range_val = "bytes=" + std::to_string(op.offset) + "-" + std::to_string(end_offset);
    extra_headers["RANGE"] = std::move(range_val);

    auto resource = get_resource(op.bucket_name, op.oid);
    sender = std::make_unique<RGWRESTStreamRWRequest>(dpp->get_cct(), "GET", op.remote_addr, cb.get(), nullptr, nullptr, "", host_style);

    ret = sender->send_request(dpp, &accessKey, extra_headers, resource, nullptr, nullptr);
    if (ret < 0) {
      r.result = ret;
      aio->put(r);
      this->r = nullptr;
    }
  };

  return this->aio->get(rgw_raw_obj{}, std::move(op_func), cost, id);
}

int RemoteCacheGetOp::complete_request(const DoutPrefixProvider* dpp, optional_yield& y)
{
  if (!sender) {
    return -EINVAL;
  }

  int ret = sender->complete_request(dpp, y);
  sender.reset();

  if (this->aio) {
    r->result = ret;
    ldpp_dout(dpp, 20) << "RemoteCacheGetOp:: " << __func__ <<  " length of buffer received: " << in_bl.length() << dendl;
    r->data = std::move(in_bl);
    this->aio->put(*r);
  }
  return ret;
}

rgw::AioResultList RemoteCacheGetOp::send_and_complete_request(const DoutPrefixProvider* dpp, rgw::Aio* aio, uint64_t cost, uint64_t id, optional_yield& y)
{
  rgw::AioResultList results = send_request(dpp, aio, cost, id, y);
  auto ret = complete_request(dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 20) << "RemoteCacheGetOp:: " << __func__ <<  " complete_request failed with ret: " << ret << dendl;
  }

  return results;
}

//placeholder for any initialization that is needed
int RemoteCacheOp::init(CephContext* cct, const DoutPrefixProvider* dpp)
{
  return 0;
}

int RemoteCacheOp::complete_request(const DoutPrefixProvider* dpp, optional_yield& y)
{
  if (!sender) {
    return -EINVAL;
  }

  int ret = sender->complete_request(dpp, y);
  sender.reset();
  return ret;
}

int RemoteCacheOp::send_and_complete_request(const DoutPrefixProvider* dpp, optional_yield& y, bufferlist* bl)
{
  auto ret = send_request(dpp, y, bl);
  if (ret < 0) {
    ldpp_dout(dpp, 20) << "RemoteCachePutOp:: " << __func__ <<  "(): send_request failed with ret: " << ret << dendl;
    return ret;
  }

  ret = complete_request(dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 20) << "RemoteCachePutOp:: " << __func__ << "(): complete_request failed with ret: " << ret << dendl;
    return ret;
  }

  return 0;
}

int RemoteCacheDeleteOp::send_request(const DoutPrefixProvider* dpp, optional_yield& y, bufferlist* bl)
{
  in_bl.clear();
  cb = std::make_unique<RemoteGetCB>(&in_bl);

  RGWAccessKey accessKey;
  std::string findKey;

  std::unique_ptr<rgw::sal::User> c_user = driver->get_user(op.bucket_owner);
  int ret = c_user->load_user(dpp, y);
  if (ret < 0) {
    return -EPERM;
  }

  if (c_user->get_info().access_keys.empty()) {
    return -EINVAL;
  }

  accessKey.id = c_user->get_info().access_keys.begin()->second.id;
  accessKey.key = c_user->get_info().access_keys.begin()->second.key;

  HostStyle host_style = PathStyle;
  std::map<std::string, std::string> extra_headers;
  extra_headers["x-rgw-remote-cache-request"] = "true";
  extra_headers["x-rgw-cache-object-version"] = op.version;
  extra_headers["x-rgw-cache-blk-offset"] = std::to_string(op.offset);
  extra_headers["x-rgw-cache-blk-len"] = std::to_string(op.len);
  extra_headers["x-rgw-cache-obj-size"] = std::to_string(op.obj_size);

  auto resource = get_resource(op.bucket_name, op.oid);
  sender = std::make_unique<RGWRESTStreamRWRequest>(dpp->get_cct(), "DELETE", op.remote_addr, cb.get(), nullptr, nullptr, "", host_style);

  ret = sender->send_request(dpp, &accessKey, extra_headers, resource, nullptr, bl);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int RemoteCachePutOp::send_request(const DoutPrefixProvider* dpp, optional_yield& y, bufferlist* bl)
{
  in_bl.clear();
  cb = std::make_unique<RemoteGetCB>(&in_bl);

  RGWAccessKey accessKey;
  std::string findKey;

  std::unique_ptr<rgw::sal::User> c_user = driver->get_user(op.bucket_owner);
  int ret = c_user->load_user(dpp, y);
  if (ret < 0) {
    return -EPERM;
  }

  if (c_user->get_info().access_keys.empty()) {
    return -EINVAL;
  }

  accessKey.id = c_user->get_info().access_keys.begin()->second.id;
  accessKey.key = c_user->get_info().access_keys.begin()->second.key;

  HostStyle host_style = PathStyle;
  std::map<std::string, std::string> extra_headers;
  extra_headers["x-rgw-remote-cache-request"] = "true";
  extra_headers["x-rgw-cache-object-version"] = op.version;
  extra_headers["x-rgw-cache-blk-offset"] = std::to_string(op.offset);
  extra_headers["x-rgw-cache-blk-len"] = std::to_string(op.len);
  extra_headers["x-rgw-cache-obj-size"] = std::to_string(op.obj_size);

  auto resource = get_resource(op.bucket_name, op.oid);
  sender = std::make_unique<RGWRESTStreamRWRequest>(dpp->get_cct(), "PUT", op.remote_addr, cb.get(), nullptr, nullptr, "", host_style);

  return sender->send_request(dpp, &accessKey, extra_headers, resource, nullptr, bl);
}

int RemoteCachePutBatch::send(const DoutPrefixProvider* dpp,
                              optional_yield y,
                              RemoteCachePutOp::RemoteCachePutOpData& op,
                              bufferlist& bl)
{
  // Drain one if at capacity
  while (in_flight.size() >= max_in_flight) {
    int ret = complete_next(dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, 5) << "RemoteCachePutOp request failed for oid=" << op.oid << " ret=" << ret << dendl;
    }
  }

  // Create and initialize the put operation
  auto put_op = std::make_unique<RemoteCachePutOp>(driver, op);
  int ret = put_op->init(cct, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "Failed to init RemoteCachePutOp for oid=" << op.oid << " ret=" << ret << dendl;
    return ret;
  }

  // Send the request
  ret = put_op->send_request(dpp, y, &bl);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "Failed to send RemoteCachePutOp for oid=" << op.oid << " ret=" << ret << dendl;
    return ret;
  }

  // Track it
  PutResult result {
    .put_op = std::move(put_op),
    .key = op.oid,
    .op_info = op
  };
  in_flight.push_back(std::move(result));

  ldpp_dout(dpp, 20) << "RemoteCachePutOp queued: oid=" << op.oid << " offset=" << op.offset << " len=" << op.len << dendl;

  return 0;
}

int RemoteCachePutBatch::complete_next(const DoutPrefixProvider* dpp, optional_yield y)
{
  if (in_flight.empty()) {
    return 0;
  }

  auto result = std::move(in_flight.front());
  in_flight.pop_front();

  // Complete the request
  result.status = result.put_op->complete_request(dpp, y);

  if (result.status < 0) {
    ldpp_dout(dpp, 5) << "RemoteCachePut completed with error: oid=" << result.key << " ret=" << result.status << dendl;
  } else {
    ldpp_dout(dpp, 20) << "RemoteCachePut completed successfully: oid=" << result.key << dendl;
  }

  result.put_op.reset();

  completed.push_back(std::move(result));
  return result.status;
}

int RemoteCachePutBatch::finish_all(const DoutPrefixProvider* dpp, optional_yield y)
{
  int first_error = 0;
  int error_count = 0;

  while (!in_flight.empty()) {
    int ret = complete_next(dpp, y);
    if (ret < 0) {
      error_count++;
      if (first_error == 0) {
        first_error = ret;
      }
    }
  }

  if (error_count > 0) {
    ldpp_dout(dpp, 5) << "RemoteCachePutBatch finished with " << error_count << " errors" << dendl;
  }

  return first_error;
}

} } // namespace rgw::d4n
