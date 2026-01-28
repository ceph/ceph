#include "d4n_remote_cache_manager.h"

#include "../../../common/async/yield_context.h"
#include "common/async/blocked_completion.h"

namespace rgw { namespace d4n {

//placeholder for any initialization that is needed
int RemoteCachePut::init(CephContext* cct, const DoutPrefixProvider* dpp)
{
  return 0;
}

int RemoteCachePut::send_request(const DoutPrefixProvider* dpp, bufferlist& bl, optional_yield& y)
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

  ret = sender->send_request(dpp, &accessKey, extra_headers, resource, nullptr, &bl);
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int RemoteCachePut::complete_request(const DoutPrefixProvider* dpp, optional_yield& y)
{
  if (!sender) {
    return -EINVAL;
  }

  int ret = sender->complete_request(dpp, y);
  sender.reset();
  return ret;
}

int RemoteCachePut::send_and_complete_request(const DoutPrefixProvider* dpp, bufferlist& bl, optional_yield& y)
{
  auto ret = send_request(dpp, bl, y);
  if (ret < 0) {
    ldpp_dout(dpp, 20) << "RemoteCachePut:: " << __func__ <<  " send_request failed with ret: " << ret << dendl;
    return ret;
  }

  ret = complete_request(dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 20) << "RemoteCachePut:: " << __func__ <<  " complete_request failed with ret: " << ret << dendl;
    return ret;
  }

  return 0;
}

int RemoteCachePutBatch::send(const DoutPrefixProvider* dpp,
                              optional_yield y,
                              RemoteCachePut::RemoteCachePutOp& op,
                              bufferlist& bl)
{
  // Drain one if at capacity
  while (in_flight.size() >= max_in_flight) {
    int ret = complete_next(dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, 5) << "RemoteCachePut request failed for oid=" << op.oid << " ret=" << ret << dendl;
    }
  }

  // Create and initialize the put operation
  auto put_op = std::make_unique<RemoteCachePut>(driver, op);
  int ret = put_op->init(cct, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "Failed to init RemoteCachePut for oid=" << op.oid << " ret=" << ret << dendl;
    return ret;
  }

  // Send the request
  ret = put_op->send_request(dpp, bl, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "Failed to send RemoteCachePut for oid=" << op.oid << " ret=" << ret << dendl;
    return ret;
  }

  // Track it
  PutResult result;
  result.put_op = std::move(put_op);
  result.key = op.oid;
  result.op_info = op;
  in_flight.push_back(std::move(result));

  ldpp_dout(dpp, 20) << "RemoteCachePut queued: oid=" << op.oid << " offset=" << op.offset << " len=" << op.len << dendl;

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
