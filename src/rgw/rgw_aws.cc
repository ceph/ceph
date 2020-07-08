#include "rgw_aws.h"
#include <string>
#include <iostream>
#include <utility>
#include "common/dout.h"
#include <aws/core/Aws.h>
#include <aws/sns/SNSClient.h>
#include <aws/lambda/LambdaClient.h>
#include <aws/lambda/model/InvokeRequest.h>
#include <boost/lockfree/queue.hpp>
#include <aws/core/utils/ARN.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/sns/model/PublishRequest.h>

#define dout_subsys ceph_subsys_rgw

namespace rgw::aws {
  static const int STATUS_OK = 1;
  static const int STATUS_FULL = 2;
  static const int STATUS_MANAGER_STOPPED = 3;
  static const int STATUS_CLIENT_CLOSED = 4;

  struct client_id_t {
    const std::string accessKey;
    const std::string accessSecret;
    const std::string arn;
    const bool useSSL;

    client_id_t(const std::string accessKey, const std::string accessSecret,
                const std::string arn, bool useSsl) : accessKey(accessKey),
                                                      accessSecret(accessSecret),
                                                      arn(arn), useSSL(useSsl) {}

    bool operator==(const client_id_t &other) const {
      return accessKey == other.accessKey &&
             accessSecret == other.accessSecret &&
             arn == other.arn && useSSL == other.useSSL;
    }

    struct hasher {
      std::size_t operator()(const client_id_t &k) const {
        return ((std::hash<std::string>()(k.accessKey)
                 ^ (std::hash<std::string>()(k.accessSecret) << 1)) >> 1)
               ^ (std::hash<std::string>()(k.arn) << 1)
               ^ (std::hash<bool>()(k.useSSL) << 1);
      }
    };


  };

  struct message_t {
    const AwsClient client;
    const std::string resourceARN;
    const std::string payload;

    message_t(const AwsClient client, const std::string& resourceARN, const std::string& payload) :
            client(client),
            resourceARN(resourceARN),
            payload(payload) {};
  };

  typedef std::unordered_map<client_id_t, AwsClient, client_id_t::hasher> ClientList;


  typedef boost::lockfree::queue<message_t *, boost::lockfree::fixed_sized<true>> MessageQueue;

  class Manager {
  private:
    MessageQueue messageQueue;
    ClientList clientList;
    std::thread runner;
    bool stopped;
    mutable std::mutex clientsLock;
    CephContext *cct;
    const ceph::coarse_real_clock::duration idle_time;
    const int MAX_CONNECTIONS = 25;
    const int REQUEST_TIMEOUT_MS = 3000;
    const int CONNECT_TIMEOUT_MS = 3000;

    void publish_internal_wrapper(message_t *msg) {
      const std::unique_ptr<message_t> msg_owner(msg);

      std::visit([&msg, this](auto &&arg) { this->publish_internal(arg, msg->resourceARN, msg->payload); }, msg->client);
    }

    void publish_internal(Aws::SNS::SNSClient *client, const std::string &resourceArn, const std::string &payload) {
      ldout(cct, 20) << "Publishing.." << dendl;
      Aws::SNS::Model::PublishRequest req;
      req.SetMessage(Aws::String(payload));
      req.SetTopicArn(Aws::String(resourceArn));

      auto result = client->Publish(req);
      if (result.IsSuccess()) {
        ldout(cct, 20) << "Published..." << dendl;
      } else {
        ldout(cct, 1) << result.GetError().GetMessage() << dendl;
      }
    }

    void publish_internal(Aws::Lambda::LambdaClient *client, const std::string &resourceArn, const std::string &payload) {
      ldout(cct, 20) << "Invoking..." << dendl;
      Aws::Lambda::Model::InvokeRequest invokeRequest;
      Aws::Utils::ARN arn((Aws::String(resourceArn)));
      invokeRequest.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);
      invokeRequest.SetLogType(Aws::Lambda::Model::LogType::Tail);
      invokeRequest.SetFunctionName(arn.GetResource());
      std::shared_ptr<Aws::IOStream> body = Aws::MakeShared<Aws::StringStream>("Simple Allocation Tag");
      *body << payload;
      invokeRequest.SetBody(body);
      invokeRequest.SetContentType("application/json");
      auto response = client->Invoke(invokeRequest);
      if (!response.IsSuccess()) {
        ldout(cct, 1) << response.GetError().GetMessage() << dendl;
      } else {
        Aws::Lambda::Model::InvokeResult &result = response.GetResult();
        Aws::IOStream &pl = result.GetPayload();
        Aws::String returnedData;
        std::getline(pl, returnedData);
        ldout(cct, 20) << "Success " << returnedData << "\n" << dendl;
      }
    }

//    Works on messages in the queque
    void run() {
      while (!stopped) {
        const auto count = messageQueue.consume_all(
                std::bind(&Manager::publish_internal_wrapper, this, std::placeholders::_1));
        if (count == 0) {
          std::this_thread::sleep_for(idle_time);
        }
      }
    }

    // used in the dtor for message cleanup
    static void delete_message(const message_t *message) {
      delete message;
    }

  public:
    Manager(size_t _max_clients, size_t max_queue_size,unsigned idle_time_ms, CephContext *_cct) : messageQueue(max_queue_size),
                                                                                                   clientList(_max_clients),
                                                                                                   runner(&Manager::run, this),
                                                                                                   stopped(false), cct(_cct),
                                                                                                   idle_time(std::chrono::milliseconds(idle_time_ms)){}
    // dtor wait for thread to stop
    // then connection are cleaned-up
    ~Manager() {
      stopped = true;
      runner.join();
      messageQueue.consume_all(delete_message);
    }


    AwsClient connect(const std::string &accessKey,
                      const std::string &accessSecret,
                      const std::string &arn,
                      const std::string &caPath,
                      bool verifySSL,
                      bool useSSL) {
      if (stopped) {
        return NO_CLIENT;
      }
      const client_id_t id(accessKey, accessSecret, arn, useSSL);
      std::lock_guard lock(clientsLock);
      const auto it = clientList.find(id);
      if (it != clientList.end()) {
        return it->second;
      }

      // connection not found, creating a new one
      Aws::Client::ClientConfiguration configuration;
      if (useSSL) {
        configuration.scheme = Aws::Http::Scheme::HTTPS;
      } else {
        configuration.scheme = Aws::Http::Scheme::HTTP;
      }
      configuration.verifySSL = verifySSL;
      Aws::Utils::ARN arnAWS((Aws::String(arn)));
      configuration.region = Aws::String(arnAWS.GetRegion());
      configuration.maxConnections = MAX_CONNECTIONS;
      configuration.requestTimeoutMs = REQUEST_TIMEOUT_MS;
      configuration.connectTimeoutMs = CONNECT_TIMEOUT_MS;
      configuration.caPath = Aws::String(caPath);
      Aws::Auth::AWSCredentials credentials;
      credentials.SetAWSAccessKeyId(Aws::String(accessKey));
      credentials.SetAWSSecretKey(Aws::String(accessSecret));
      AwsClient client;
      if (arnAWS.GetService() == "lambda") {
        client = new Aws::Lambda::LambdaClient(credentials, configuration);
      } else if (arnAWS.GetService() == "sns") {
        client = new Aws::SNS::SNSClient(credentials, configuration);
      } else {
        return NO_CLIENT;
      }
      return clientList.emplace(id, client).first->second;
    }

    int publish(AwsClient client,
                const std::string &payload,
                const std::string &resourceARN) {
      if (stopped) {
        return STATUS_MANAGER_STOPPED;
      }
      if (client == NO_CLIENT) {
        return STATUS_CLIENT_CLOSED;
      }
      if (messageQueue.push(new message_t(client, resourceARN, payload))) {
        return STATUS_OK;
      }


      return STATUS_FULL;
    }

    void stop() {
      stopped = true;
    }

  };

  static Manager *manager = nullptr;
  Aws::SDKOptions options;
  static const unsigned IDLE_TIME_MS = 100;
  static const size_t MAX_QUEUE_DEFAULT = 8192;
  static const size_t MAX_CLIENTS_DEFAULT = 256;

  bool init(CephContext *cct) {
    if (manager) {
      return false;
    }
    options = Aws::SDKOptions();
    Aws::InitAPI(options);
    manager = new Manager(MAX_CLIENTS_DEFAULT, MAX_QUEUE_DEFAULT, IDLE_TIME_MS, cct);
    return true;
  }

  void shutdown() {
    delete manager;
    Aws::ShutdownAPI(options);
    manager = nullptr;
  }

  AwsClient connect(const std::string &accessKey,
                    const std::string &accessSecret,
                    const std::string &arn,
                    const std::string &caPath,
                    bool verifySSL,
                    bool useSSL) {
    if (!manager) return NO_CLIENT;
    return manager->connect(accessKey, accessSecret, arn, caPath, verifySSL, useSSL);
  }

  // Publishes the message to the queue
  int publish(const AwsClient client,
              const std::string &payload,
              const std::string &resourceARN) {
    if (!manager)
      return STATUS_MANAGER_STOPPED;
    return manager->publish(client, payload, resourceARN);
  }
}
