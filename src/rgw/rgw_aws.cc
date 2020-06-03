#include "rgw_url.h"
#include "rgw_aws.h"
#include "rgw_arn.h"
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
  static const int STATUS_OK = 0x0;
  static const int STATUS_FULL = -0x1003;
  static const int STATUS_MANAGER_STOPPED = -0x1005;
  static const int STATUS_CLIENT_CLOSED = -0x1002;

  struct client_id_t {
    const std::string access_key;
    const std::string access_secret;
    const std::string arn;
    const bool use_ssl;

    client_id_t(const std::string &access_key, const std::string &access_secret,
                const std::string &arn, bool useSsl) : access_key(access_key),
                                                       access_secret(access_secret),
                                                       arn(arn), use_ssl(useSsl) {}

    bool operator==(const client_id_t &other) const {
      return access_key == other.access_key &&
             access_secret == other.access_secret &&
             arn == other.arn && use_ssl == other.use_ssl;
    }

    struct hasher {
      std::size_t operator()(const client_id_t &k) const {
        return ((std::hash<std::string>()(k.access_key)
                 ^ (std::hash<std::string>()(k.access_secret) << 1)) >> 1)
               ^ (std::hash<std::string>()(k.arn) << 1)
               ^ (std::hash<bool>()(k.use_ssl) << 1);
      }
    };


  };
  class AsyncCallerContext: public Aws::Client::AsyncCallerContext{
  public:
    const reply_callback_t cb;
    CephContext* const cct;
    AsyncCallerContext(reply_callback_t cb, CephContext* cct):cb(cb), cct(cct){}
  };

  void invoke_callback(const Aws::Lambda::LambdaClient* client,
                              const Aws::Lambda::Model::InvokeRequest& request,
                              const Aws::Lambda::Model::InvokeOutcome & outcome,
                              const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context){

    auto ctx = static_cast<const AsyncCallerContext*>(context.get());
    CephContext* cct = ctx->cct;
    if (outcome.IsSuccess()) {
      ldout(cct, 20) << "Invoke function: " << request.GetFunctionName() << " Response: " << outcome.GetResult().GetLogResult() << dendl;
      if(ctx && ctx->cb) {
        ctx->cb(STATUS_OK);
      }
    }
    else {
      const auto& error = outcome.GetError();
      ldout(cct, 20) << "ERROR: " << error.GetExceptionName() << ": "
                << error.GetMessage() << dendl;
      if(ctx && ctx->cb) {
        ctx->cb(-static_cast<int>(error.GetErrorType()));
      }
    }

  }
  void publish_callback(const Aws::SNS::SNSClient* client,
                        const Aws::SNS::Model::PublishRequest& request,
                        const Aws::SNS::Model::PublishOutcome & outcome,
                        const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context){

    auto ctx = static_cast<const AsyncCallerContext*>(context.get());
    CephContext* cct = ctx->cct;
    if (outcome.IsSuccess()) {
      ldout(cct, 20) << "SNS: published to topic: " << request.GetTopicArn() << dendl;
      if(ctx && ctx->cb) {
        ctx->cb(STATUS_OK);
      }

    }
    else {
      const auto& error = outcome.GetError();
      ldout(cct, 20) << "ERROR: " << error.GetExceptionName() << ": "
                << error.GetMessage() << dendl;
      if(ctx && ctx->cb) {
        ctx->cb(-static_cast<int>(error.GetErrorType()));
      }
    }
  }

  struct message_t {
    const AwsClient client;
    const std::string resource_arn;
    const std::string payload;
    const reply_callback_t cb;

    message_t(const AwsClient client, const std::string& resource_arn, const std::string& payload, reply_callback_t cb) :
            client(client),
            resource_arn(resource_arn),
            payload(payload),
            cb(cb) {};
  };

  typedef std::unordered_map<client_id_t, AwsClient, client_id_t::hasher> ClientList;


  typedef boost::lockfree::queue<message_t *, boost::lockfree::fixed_sized<true>> MessageQueue;

  class Manager {
  private:
    MessageQueue message_queue;
    ClientList client_list;
    std::thread runner;
    bool stopped;
    mutable std::mutex clients_lock;
    CephContext *cct;
    const ceph::coarse_real_clock::duration idle_time;
    const int MAX_CONNECTIONS = 25;
    const int REQUEST_TIMEOUT_MS = 3000;
    const int CONNECT_TIMEOUT_MS = 3000;
    Aws::SDKOptions options;

    void publish_internal_wrapper(message_t *msg) {
      const std::unique_ptr<message_t> msg_owner(msg);

      std::visit([&msg, this](auto &&arg) { this->publish_internal(arg, msg->resource_arn, msg->payload, msg->cb); }, msg->client);
    }

    void publish_internal(Aws::SNS::SNSClient *client, const std::string &resource_arn, const std::string &payload, reply_callback_t cb) {
      Aws::SNS::Model::PublishRequest req;
      req.SetMessage(Aws::String(payload));
      req.SetTopicArn(Aws::String(resource_arn));

      auto context = Aws::MakeShared<AsyncCallerContext>("PublishAllocationTag", cb, cct);
      client->PublishAsync(req, publish_callback, context);
    }

    void publish_internal(Aws::Lambda::LambdaClient *client, const std::string &resource_arn, const std::string &payload, reply_callback_t cb) {
      Aws::Lambda::Model::InvokeRequest invokeRequest;
      Aws::Utils::ARN arn((Aws::String(resource_arn)));
      invokeRequest.SetInvocationType(Aws::Lambda::Model::InvocationType::RequestResponse);
      invokeRequest.SetLogType(Aws::Lambda::Model::LogType::Tail);
      boost::optional<ARNResource> resourceParsed = ARNResource::parse(std::string(arn.GetResource()));
      if(!resourceParsed) {
        invokeRequest.SetFunctionName("");
      }else{
        invokeRequest.SetFunctionName(Aws::String(resourceParsed.get().resource));
      }
      std::shared_ptr<Aws::IOStream> body = Aws::MakeShared<Aws::StringStream>("Simple Allocation Tag");
      *body << payload;
      invokeRequest.SetBody(body);
      invokeRequest.SetContentType("application/json");
      auto context = Aws::MakeShared<AsyncCallerContext>("InvokeAllocationTag", cb, cct);
      client->InvokeAsync(invokeRequest, invoke_callback, context);
    }

//    Works on messages in the queque
    void run() {
      while (!stopped) {
        const auto count = message_queue.consume_all(
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
    Manager(size_t _max_clients, size_t max_queue_size,unsigned idle_time_ms, CephContext *_cct) : message_queue(max_queue_size),
                                                                                                   client_list(_max_clients),
                                                                                                   runner(&Manager::run, this),
                                                                                                   stopped(false), cct(_cct),
                                                                                                   idle_time(std::chrono::milliseconds(idle_time_ms)){
          options = Aws::SDKOptions();
          Aws::InitAPI(options);
    }
    // dtor wait for thread to stop
    // then connection are cleaned-up
    ~Manager() {
      stopped = true;
      runner.join();
      message_queue.consume_all(delete_message);
      Aws::ShutdownAPI(options);
    }


    AwsClient connect(const std::string &access_key,
                      const std::string &access_secret,
                      const std::string &arn,
                      const std::string &endpoint,
                      bool verify_ssl,
                      bool use_ssl) {
      if (stopped) {
        return NO_CLIENT;
      }
      const client_id_t id(access_key, access_secret, arn, use_ssl);
      std::lock_guard lock(clients_lock);
      const auto it = client_list.find(id);
      if (it != client_list.end()) {
        return it->second;
      }

      // connection not found, creating a new one
      Aws::Client::ClientConfiguration configuration;
      if (use_ssl) {
        configuration.scheme = Aws::Http::Scheme::HTTPS;
      } else {
        configuration.scheme = Aws::Http::Scheme::HTTP;
      }
      configuration.verifySSL = verify_ssl;
      Aws::Utils::ARN arnAWS((Aws::String(arn)));
      configuration.region = Aws::String(arnAWS.GetRegion());
      configuration.maxConnections = MAX_CONNECTIONS;
      configuration.requestTimeoutMs = REQUEST_TIMEOUT_MS;
      configuration.connectTimeoutMs = CONNECT_TIMEOUT_MS;
      std::string host, user, password;
      rgw::parse_url_authority(endpoint, host, user, password);
      configuration.endpointOverride = Aws::String(host);
      Aws::Auth::AWSCredentials credentials;
      credentials.SetAWSAccessKeyId(Aws::String(access_key));
      credentials.SetAWSSecretKey(Aws::String(access_secret));
      AwsClient client;
      if (arnAWS.GetService() == "lambda") {
        client = new Aws::Lambda::LambdaClient(credentials, configuration);
      } else if (arnAWS.GetService() == "sns") {
        client = new Aws::SNS::SNSClient(credentials, configuration);
      } else {
        return NO_CLIENT;
      }
      return client_list.emplace(id, client).first->second;
    }

    int publish(AwsClient client,
                const std::string &payload,
                const std::string &resource_arn,
                reply_callback_t cb) {
      if (stopped) {
        return STATUS_MANAGER_STOPPED;
      }
      if (client == NO_CLIENT) {
        return STATUS_CLIENT_CLOSED;
      }
      if (message_queue.push(new message_t(client, resource_arn, payload, cb))) {
        return STATUS_OK;
      }


      return STATUS_FULL;
    }

    void stop() {
      stopped = true;
    }

  };

  static Manager *manager = nullptr;
  static const unsigned IDLE_TIME_MS = 100;
  static const size_t MAX_QUEUE_DEFAULT = 8192;
  static const size_t MAX_CLIENTS_DEFAULT = 256;

  bool init(CephContext *cct) {
    if (manager) {
      return false;
    }
    manager = new Manager(MAX_CLIENTS_DEFAULT, MAX_QUEUE_DEFAULT, IDLE_TIME_MS, cct);
    return true;
  }

  void shutdown() {
    delete manager;
    manager = nullptr;
  }

  AwsClient connect(const std::string &access_key,
                    const std::string &access_secret,
                    const std::string &arn,
                    const std::string &endpoint,
                    bool verifySSL,
                    bool useSSL) {
    if (!manager) return NO_CLIENT;
    return manager->connect(access_key, access_secret, arn, endpoint, verifySSL, useSSL);
  }

  // Publishes the message to the queue
  int publish(const AwsClient client,
              const std::string &payload,
              const std::string &resource_arn,
              reply_callback_t cb) {
    if (!manager)
      return STATUS_MANAGER_STOPPED;
    return manager->publish(client, payload, resource_arn, cb);
  }
}
