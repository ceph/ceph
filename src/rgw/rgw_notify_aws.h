#pragma once

#include <string>
#include <variant>
#include <functional>
#include "include/common_fwd.h"

namespace Aws {
  namespace Lambda {
    class LambdaClient;
  }
  namespace SNS {
    class SNSClient;
  }
}

namespace rgw::aws {

  typedef std::variant<Aws::Lambda::LambdaClient *, Aws::SNS::SNSClient *> AwsClient;
  typedef std::function<void(int)> reply_callback_t;

  static const AwsClient NO_CLIENT = AwsClient();

  AwsClient connect(const std::string &access_key,
                    const std::string &access_secret,
                    const std::string &arn,
                    const std::string &endpoint,
                    bool verifySSL,
                    bool useSSL);

  int publish(AwsClient client,
              const std::string &payload,
              const std::string &resource_arn,
              reply_callback_t cb);

  bool init(CephContext *cct);

  void shutdown();
}

