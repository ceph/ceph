#pragma once

#include <string>
#include <variant>
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

  static const AwsClient NO_CLIENT = AwsClient();

  AwsClient connect(const std::string &accessKey,
                    const std::string &accessSecret,
                    const std::string &arn,
                    const std::string &caPath,
                    bool verifySSL,
                    bool useSSL);

  int publish(AwsClient client,
              const std::string &payload,
              const std::string &resourceARN);

  bool init(CephContext *cct);

  void shutdown();
}

