#include <iostream>
#include <map>

#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "common/Formatter.h"
#include "common/ceph_json.h"
#include "common/ceph_time.h"
#include "global/global_init.h"
#include "include/assert.h"
#include "include/str_list.h"

#include "rgw_sts.h"

using namespace std;

void help()
{
  cout<<"option:  "<<"--help  help"<< std::endl;
  cout<<"         "<<"--role-id  role id"<< std::endl;
  cout<<"         "<<"--policy  policy"<< std::endl;
  cout<<"         "<<"--saml-keys  saml keys"<< std::endl;
  cout<<"         "<<"--duration  duration in seconds"<< std::endl;
}

void string_to_map(const std::string& input, std::map<std::string, std::string>& saml_map)
{
  std::string token, key, value;
  std::istringstream iss(input);

  while (std::getline(iss, token, ',')) {
  token.erase(std::remove_if( token.begin(), token.end(),
    [](char c){ return (c =='\r' || c =='\t' || c == ' ' || c == '\n' || c == '{' || c == '}' || c == '"');}), token.end() );
    auto pos = token.find(':');
    key = token.substr(0, pos);
    value = token.substr(pos + 1);
    saml_map[key] = value;
  }
}

int main(int argc,char *argv[]) {

  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY, 0);

  common_init_finish(g_ceph_context);

  std::string role_id, policy, val, err;
  int duration = 0, ret;
  std::map<std::string, std::string> saml_map;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_flag(args, i, &val, "--help", (char*)NULL)) {
      help();
      return 0;
    } else if (ceph_argparse_witharg(args, i, &val, "--role-id", (char*)NULL)) {
      role_id = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--policy", (char*)NULL)) {
      policy = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--duration", (char*)NULL)) {
      duration = (int)strict_strtol(val.c_str(), 10, &err);
      if (!err.empty()) {
        cout << "ERROR: Invalid argument: " << duration << std::endl;
        return -EINVAL;
      }
    } else if (ceph_argparse_witharg(args, i, &val, "--saml-keys", (char*)NULL)) {
      string_to_map(val, saml_map);
    }
  }

  RGWSts rgw_sts(duration, role_id, policy, saml_map, g_ceph_context);
  ret = rgw_sts.validate_input();
  if (ret < 0) {
    return ret;
  }

  RGWSts::sts output;
  err = rgw_sts.build_output(output);
  if (ret < 0) {
    return ret;
  }

  JSONFormatter formatter(true);
  formatter.open_object_section("AssumeRoleWithSaml");
  output.dump(&formatter);
  formatter.close_section();
  formatter.flush(cout);

  return 0;
}

