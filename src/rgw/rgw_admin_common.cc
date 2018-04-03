#include "rgw_admin_common.h"

#include "cls/rgw/cls_rgw_types.h"

#include "common/ceph_json.h"
#include <common/errno.h>
#include <common/safe_io.h>

#include "rgw_common.h"
#include "rgw_rados.h"

#define dout_subsys ceph_subsys_rgw

int init_bucket(RGWRados *store, const std::string& tenant_name, const std::string& bucket_name, const std::string& bucket_id,
                RGWBucketInfo& bucket_info, rgw_bucket& bucket, map<std::string, bufferlist> *pattrs)
{
  if (!bucket_name.empty()) {
    RGWObjectCtx obj_ctx(store);
    int r;
    if (bucket_id.empty()) {
      r = store->get_bucket_info(obj_ctx, tenant_name, bucket_name, bucket_info, nullptr, pattrs);
    } else {
      std::string bucket_instance_id = bucket_name + ":" + bucket_id;
      r = store->get_bucket_instance_info(obj_ctx, bucket_instance_id, bucket_info, nullptr, pattrs);
    }
    if (r < 0) {
      cerr << "could not get bucket info for bucket=" << bucket_name << std::endl;
      return r;
    }
    bucket = bucket_info.bucket;
  }
  return 0;
}

int read_input(const std::string& infile, bufferlist& bl)
{
  const int READ_CHUNK = 8196;
  int fd = 0;
  if (!infile.empty()) {
    fd = open(infile.c_str(), O_RDONLY);
    if (fd < 0) {
      int err = -errno;
      cerr << "error reading input file " << infile << std::endl;
      return err;
    }
  }

  int r;
  int err;

  do {
    char buf[READ_CHUNK];

    r = safe_read(fd, buf, READ_CHUNK);
    if (r < 0) {
      err = -errno;
      cerr << "error while reading input" << std::endl;
      goto out;
    }
    bl.append(buf, r);
  } while (r > 0);
  err = 0;

  out:
  if (!infile.empty()) {
    close(fd);
  }
  return err;
}

int parse_date_str(const std::string& date_str, utime_t& ut)
{
  uint64_t epoch = 0;
  uint64_t nsec = 0;

  if (!date_str.empty()) {
    int ret = utime_t::parse_date(date_str, &epoch, &nsec);
    if (ret < 0) {
      cerr << "ERROR: failed to parse date: " << date_str << std::endl;
      return -EINVAL;
    }
  }

  ut = utime_t(epoch, nsec);

  return 0;
}

int check_min_obj_stripe_size(RGWRados *store, RGWBucketInfo& bucket_info, rgw_obj& obj, uint64_t min_stripe_size, bool *need_rewrite)
{
  map<std::string, bufferlist> attrs;
  uint64_t obj_size;

  RGWObjectCtx obj_ctx(store);
  RGWRados::Object op_target(store, bucket_info, obj_ctx, obj);
  RGWRados::Object::Read read_op(&op_target);

  read_op.params.attrs = &attrs;
  read_op.params.obj_size = &obj_size;

  int ret = read_op.prepare();
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: failed to stat object, returned error: " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  map<std::string, bufferlist>::iterator iter;
  iter = attrs.find(RGW_ATTR_MANIFEST);
  if (iter == attrs.end()) {
    *need_rewrite = (obj_size >= min_stripe_size);
    return 0;
  }

  RGWObjManifest manifest;

  try {
    bufferlist& bl = iter->second;
    bufferlist::iterator biter = bl.begin();
    ::decode(manifest, biter);
  } catch (buffer::error& err) {
    ldout(store->ctx(), 0) << "ERROR: failed to decode manifest" << dendl;
    return -EIO;
  }

  map<uint64_t, RGWObjManifestPart>& objs = manifest.get_explicit_objs();
  map<uint64_t, RGWObjManifestPart>::iterator oiter;
  for (oiter = objs.begin(); oiter != objs.end(); ++oiter) {
    RGWObjManifestPart& part = oiter->second;

    if (part.size >= min_stripe_size) {
      *need_rewrite = true;
      return 0;
    }
  }
  *need_rewrite = false;

  return 0;
}

int read_current_period_id(RGWRados* store, const std::string& realm_id,
                           const std::string& realm_name,
                           std::string* period_id)
{
  RGWRealm realm(realm_id, realm_name);
  int ret = realm.init(g_ceph_context, store);
  if (ret < 0) {
    std::cerr << "failed to read realm: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  *period_id = realm.get_current_period();
  return 0;
}

int check_reshard_bucket_params(RGWRados *store,
                                const std::string& bucket_name,
                                const std::string& tenant,
                                const std::string& bucket_id,
                                bool num_shards_specified,
                                int num_shards,
                                int yes_i_really_mean_it,
                                rgw_bucket& bucket,
                                RGWBucketInfo& bucket_info,
                                map<std::string, bufferlist>& attrs)
{
  if (bucket_name.empty()) {
    cerr << "ERROR: bucket not specified" << std::endl;
    return -EINVAL;
  }

  if (!num_shards_specified) {
    cerr << "ERROR: --num-shards not specified" << std::endl;
    return -EINVAL;
  }

  if (num_shards > (int)store->get_max_bucket_shards()) {
    cerr << "ERROR: num_shards too high, max value: " << store->get_max_bucket_shards() << std::endl;
    return -EINVAL;
  }

  int ret = init_bucket(store, tenant, bucket_name, bucket_id, bucket_info, bucket, &attrs);
  if (ret < 0) {
    cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }

  int num_source_shards = (bucket_info.num_shards > 0 ? bucket_info.num_shards : 1);

  if (num_shards <= num_source_shards && !yes_i_really_mean_it) {
    cerr << "num shards is less or equal to current shards count" << std::endl
         << "do you really mean it? (requires --yes-i-really-mean-it)" << std::endl;
    return -EINVAL;
  }
  return 0;
}

int RgwAdminCommandGroupHandler::parse_command(boost::program_options::options_description& desc,
                                               boost::program_options::variables_map& var_map) {
  const char COMMAND[] = "command";
  std::vector<std::string> parsed_command;
  desc.add_options()
      (COMMAND, boost::program_options::value(&parsed_command), "Command");

  boost::program_options::positional_options_description pos_desc;
  pos_desc.add(COMMAND, -1);
  try {
    boost::program_options::parsed_options options = boost::program_options::command_line_parser{
        commandline_arguments.size(), commandline_arguments.data()}
        .options(desc)
        .positional(pos_desc)
        .run();

    boost::program_options::store(options, var_map);
    boost::program_options::notify(var_map);

    if (var_map.count(COMMAND)) {
      if (parsed_command.size() <= command_prefix.size()) {
        return EINVAL;
      }
      for (std::size_t i = 0; i < command_prefix.size(); ++i) {
        if (parsed_command[i] != command_prefix[i]) {
          return EINVAL;
        }
      }
      command = string_to_command.at(parsed_command[command_prefix.size()]);
    } else {
      return EINVAL;
    }
  } catch (const std::exception& ex) {
    std::cerr << "Incorrect command:" << std::endl << desc << std::endl;
    return EINVAL;
  }
  return 0;
}