// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Simple Crimson OSD Client
 * 
 * This is a minimal client to test connection and ping functionality
 * to Crimson OSD without complex authentication or messaging.
 */

#include <seastar/core/app-template.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>

#include <boost/program_options.hpp>
#include <iostream>

#include "messages/MPing.h"
#include "messages/MCommand.h"
#include "messages/MCommandReply.h"
#include "crimson/common/log.h"
#include "crimson/common/config_proxy.h"
#include "crimson/net/Dispatcher.h"
#include "crimson/net/Messenger.h"
#include "crimson/auth/AuthClient.h"
#include "crimson/net/Connection.h"
#include "include/random.h"
#include "common/entity_name.h"
#include "auth/AuthAuthorizeHandler.h"
#include "include/ceph_fs.h"
#include "msg/msg_types.h"
#include <chrono>
#include <iomanip>
#include <sstream>

using namespace std;
using namespace std::chrono_literals;

namespace bpo = boost::program_options;

seastar::logger& logger() {
  return crimson::get_logger(ceph_subsys_ms);
}

// Helper function to generate nonce
uint64_t get_nonce() {
    return ceph::util::generate_random_number<uint64_t>();
}

// Custom AuthClient for CEPH_AUTH_NONE with proper payload
class CustomAuthClient : public crimson::auth::AuthClient {
private:
    uint64_t global_id;
    EntityName entity_name;
    
public:
    CustomAuthClient() : global_id(get_nonce()) {
        entity_name.set(CEPH_ENTITY_TYPE_CLIENT, "test");
    }
    
    std::vector<uint32_t> get_supported_auth_methods(int peer_type) {
        return {CEPH_AUTH_NONE};
    }
    
    std::vector<uint32_t> get_supported_con_modes(int peer_type, uint32_t auth_method) {
        return {CEPH_CON_MODE_CRC};
    }
    
    uint32_t pick_con_mode(int peer_type, uint32_t auth_method, 
                          const std::vector<uint32_t>& preferred_modes) {
        return CEPH_CON_MODE_CRC;
    }
    
    AuthAuthorizeHandler* get_auth_authorize_handler(int peer_type, int auth_method) {
        return nullptr;
    }
    
    AuthClient::auth_request_t get_auth_request(
        crimson::net::Connection &conn,
        AuthConnectionMeta &auth_meta) override {
        ceph::bufferlist bl;
        
        // Добавляем правильную структуру для CEPH_AUTH_NONE
        // как в AuthNoneAuthorizer::build_authorizer
        __u8 struct_v = 1; // версия структуры
        bl.append((char)struct_v);
        
        // EntityName для клиента
        using ceph::encode;
        encode(entity_name, bl);
        
        // Global ID
        encode(global_id, bl);
        
        logger().info("Sending CEPH_AUTH_NONE request with {} bytes", bl.length());
        logger().info("Connecting to peer_addr: {}", conn.get_peer_addr());
        return {CEPH_AUTH_NONE, {CEPH_CON_MODE_CRC}, bl};
    }
    
    ceph::bufferlist handle_auth_reply_more(
        crimson::net::Connection &conn,
        AuthConnectionMeta &auth_meta,
        const ceph::bufferlist& bl) override {
        logger().info("handle_auth_reply_more with {} bytes", bl.length());
        return {};
    }
    
    int handle_auth_done(
        crimson::net::Connection &conn,
        AuthConnectionMeta &auth_meta,
        uint64_t global_id,
        uint32_t con_mode,
        const bufferlist& bl) override {
        logger().info("Auth done successfully, global_id={}, con_mode={}", global_id, con_mode);
        logger().info("Connection peer_addr: {}", conn.get_peer_addr());
        logger().info("Auth meta auth_method: {}", auth_meta.auth_method);
        logger().info("Auth meta con_mode: {}", auth_meta.con_mode);
        return 0;
    }
    
    int handle_auth_bad_method(
        crimson::net::Connection &conn,
        AuthConnectionMeta &auth_meta,
        uint32_t old_auth_method,
        int result,
        const std::vector<uint32_t>& allowed_methods,
        const std::vector<uint32_t>& allowed_modes) override {
        logger().warn("Auth method {} failed, result={}, allowed methods: {}", 
                     old_auth_method, result, allowed_methods);
        return -1;
    }
};


class SimpleClient : public crimson::net::Dispatcher {
private:
    crimson::net::MessengerRef messenger;
    CustomAuthClient custom_auth;
    crimson::net::ConnectionRef connection;
    entity_addr_t osd_addr;

public:
    SimpleClient() = default;
    
    seastar::future<> init(const entity_addr_t& addr) {
        osd_addr = addr;
        
        // Create messenger
        messenger = crimson::net::Messenger::create(
            entity_name_t::CLIENT(3), 
            "simple_client", 
            234354,
            true  // is_fixed_cpu
        );
        
        // Set up policy and authentication
        messenger->set_default_policy(crimson::net::SocketPolicy::lossy_client(0));
        messenger->set_auth_client(&custom_auth);
        
        // Start messenger
        crimson::net::dispatchers_t dispatchers{this};
        return messenger->start(dispatchers).then([this] {
            logger().info("Simple client initialized, connecting to {}", osd_addr);
            return connect_to_osd();
        });
    }
    
    seastar::future<> connect_to_osd() {
        try {
            connection = messenger->connect(osd_addr, entity_name_t::OSD(-1));
            logger().info("Connected to OSD at {}", osd_addr);
            return seastar::make_ready_future<>();
        } catch (const std::exception& e) {
            logger().error("Failed to connect to OSD at {}: {}", osd_addr, e.what());
            return seastar::make_exception_future<>(e);
        }
    }
    
    // Test connection
    seastar::future<bool> test_connection() {
        logger().info("Testing connection to OSD");
        
        if (connection && connection->is_connected()) {
            logger().info("Connection to OSD is alive!");
            return seastar::make_ready_future<bool>(true);
        } else {
            logger().warn("Connection to OSD is not alive");
            return seastar::make_ready_future<bool>(false);
        }
    }
    
    // Test OSD operation - send ping without waiting for reply
    seastar::future<bool> test_osd_op() {
        logger().info("Sending test OSD operation to OSD");
        
        if (!connection || !connection->is_connected()) {
            logger().error("Connection is not available or not connected");
            return seastar::make_ready_future<bool>(false);
        }
        
        // Create a simple ping message
        auto ping = crimson::make_message<MPing>();
        ping->set_priority(CEPH_MSG_PRIO_DEFAULT);
        
        logger().info("Created MPing message, type: {}", ping->get_type());
        
        auto msg_uref = std::unique_ptr<Message, crimson::common::UniquePtrDeleter>(ping.release());
        
        return connection->send(std::move(msg_uref)).then([] {
            logger().info("OSD operation sent successfully!");
            return true;
        }).handle_exception([](std::exception_ptr eptr) {
            logger().error("Failed to send OSD operation");
            return false;
        });
    }
    
    // Get OSD status operation (admin socket command)
    seastar::future<bool> get_osd_status() {
        logger().info("Getting OSD status operation");
        
        if (!connection || !connection->is_connected()) {
            logger().error("Connection is not available or not connected");
            return seastar::make_ready_future<bool>(false);
        }
        
        // Create MCommand message for OSD status
        auto cmd = crimson::make_message<MCommand>();
        cmd->set_priority(CEPH_MSG_PRIO_DEFAULT);
        
        // Use admin socket command format - based on admin_socket.cc:502-579
        // Admin socket expects JSON format with "format" and "prefix" fields
        std::string json_cmd = "{\"prefix\":\"status\",\"format\":\"json-pretty\"}";
        cmd->cmd = {json_cmd};
        
        logger().info("Created MCommand message for OSD status, type: {}", cmd->get_type());
        
        auto msg_uref = std::unique_ptr<Message, crimson::common::UniquePtrDeleter>(cmd.release());
        
        return connection->send(std::move(msg_uref)).then([] {
            logger().info("OSD status command sent successfully!");
            return true;
        }).handle_exception([](std::exception_ptr eptr) {
            logger().error("Failed to send OSD status command");
            return false;
        });
    }
    
    
    // Dispatcher interface implementation
        std::optional<seastar::future<>> ms_dispatch(crimson::net::ConnectionRef conn, 
                                                     MessageRef m) override {
        logger().info("Received message type {} from {}", m->get_type(), conn->get_peer_addr());
        
        if (m->get_type() == CEPH_MSG_PING) {
            auto ping_msg = boost::static_pointer_cast<MPing>(m);
            logger().info("Received MPing reply from OSD - SUCCESS!");
            return seastar::make_ready_future<>();
        }
        
        if (m->get_type() == MSG_COMMAND) {
            auto cmd_msg = boost::static_pointer_cast<MCommand>(m);
            logger().info("Received MCommand reply from OSD - SUCCESS!");
            logger().info("Command reply: {}", cmd_msg->get_data());
            return seastar::make_ready_future<>();
        }
        
        if (m->get_type() == MSG_COMMAND_REPLY) {
            auto cmd_reply = boost::static_pointer_cast<MCommandReply>(m);
            logger().info("Received MCommandReply from OSD - SUCCESS!");
            logger().info("Command reply code: {}", (int)cmd_reply->r);
            logger().info("Command reply message: {}", cmd_reply->rs);
            
            // Get the actual data from data field (not payload)
            if (cmd_reply->get_data().length() > 0) {
                logger().info("Command reply data length: {}", cmd_reply->get_data().length());
                logger().info("Status data: {}", cmd_reply->get_data().to_str());
            } else {
                logger().info("Command reply data is empty");
            }
            return seastar::make_ready_future<>();
        }
        
        // Log all message types to see what we're receiving
        logger().info("Unhandled message type: {} from {}", m->get_type(), conn->get_peer_addr());
        return std::nullopt;
    }
    
    void ms_handle_connect(crimson::net::ConnectionRef conn, 
                          seastar::shard_id prv_shard) override {
        logger().info("Connected to {}", conn->get_peer_addr());
    }
    
    void ms_handle_accept(crimson::net::ConnectionRef conn, 
                         seastar::shard_id prv_shard, 
                         bool is_replace) override {
        logger().info("Accepted connection from {}", conn->get_peer_addr());
    }
    
    void ms_handle_reset(crimson::net::ConnectionRef conn, bool is_replace) override {
        logger().warn("Connection reset to {}", conn->get_peer_addr());
    }
    
    void ms_handle_remote_reset(crimson::net::ConnectionRef conn) override {
        logger().warn("Remote connection reset to {}", conn->get_peer_addr());
    }
    
public:
    seastar::future<> shutdown() {
        if (messenger) {
            logger().info("Shutting down messenger...");
            // Clear dispatchers before shutdown to avoid assertion failure
            messenger->stop();
            return messenger->shutdown().then([this] {
                logger().info("Messenger shutdown completed");
                messenger = nullptr;
            });
        }
        return seastar::make_ready_future<>();
    }
};

// Main function
static seastar::future<> run(const entity_addr_t& osd_addr) {
    return seastar::do_with(SimpleClient{}, [osd_addr](auto& client) {
        return client.init(osd_addr).then([] {
            logger().info("=== Starting Simple OSD Client Test ===");
            
            // Wait for connection to be fully established
            logger().info("Waiting for connection to be established...");
            return seastar::sleep(2s);
        }).then([&client] {
            // Test connection
            logger().info("Testing connection to OSD");
            return client.test_connection();
        }).then([](bool success) {
            if (success) {
                logger().info("SUCCESS: Connection to OSD is working!");
            } else {
                logger().error("FAILED: Connection to OSD failed!");
            }
            return seastar::sleep(1s);
        }).then([&client] {
            // Test OSD operation
            logger().info("Testing OSD operation to OSD");
            return seastar::sleep(1s).then([&client] {
                return client.test_osd_op();
            });
        }).then([](bool success) {
            if (success) {
                logger().info("SUCCESS: OSD operation is working!");
            } else {
                logger().error("FAILED: OSD operation failed!");
            }
            return seastar::sleep(1s);
        }).then([&client] {
            // Test second OSD operation
            logger().info("Testing second OSD operation to OSD");
            return seastar::sleep(1s).then([&client] {
                return client.test_osd_op();
            });
        }).then([](bool success) {
            if (success) {
                logger().info("SUCCESS: Second OSD operation is working!");
            } else {
                logger().error("FAILED: Second OSD operation failed!");
            }
            return seastar::sleep(1s);
        }).then([&client] {
            // Get OSD status operation
            logger().info("Testing OSD status operation");
            return seastar::sleep(1s).then([&client] {
                return client.get_osd_status();
            });
        }).then([](bool success) {
            if (success) {
                logger().info("SUCCESS: OSD status operation is working!");
            } else {
                logger().error("FAILED: OSD status operation failed!");
            }
            return seastar::sleep(1s);
        }).finally([&client] {
            logger().info("=== Test completed, shutting down ===");
            return client.shutdown();
        });
    });
}

int main(int argc, char** argv) {
    seastar::app_template app;
    
    app.add_options()
        ("osd-addr", bpo::value<std::string>()->default_value("v2:127.0.0.1:6806"), 
         "OSD address to connect to (msgr2 format)")
        ("verbose,v", "Enable verbose logging");
    
    // Try with minimal Seastar configuration
    const char *seastar_args[] = { argv[0], "--smp", "1" };
    return app.run(
        sizeof(seastar_args) / sizeof(seastar_args[0]),
        const_cast<char**>(seastar_args),
        [&app] {
        auto& config = app.configuration();
        
        // Set up logging
        if (config.count("verbose")) {
            seastar::global_logger_registry().set_all_loggers_level(seastar::log_level::debug);
        }
        
        // Parse OSD address
        std::string addr_str = config["osd-addr"].as<std::string>();
        entity_addr_t osd_addr;
        
        if (!osd_addr.parse(addr_str.c_str(), nullptr)) {
            std::cerr << "Invalid address format: " << addr_str << std::endl;
            std::cerr << "Use msgr2 format like: v2:127.0.0.1:6806" << std::endl;
            return seastar::make_ready_future<int>(1);
        }
        
        if (!osd_addr.is_msgr2()) {
            std::cerr << "Address must be msgr2 format" << std::endl;
            return seastar::make_ready_future<int>(1);
        }
        
 
        osd_addr.set_nonce(1854381601);
       
        std::cout << "Simple Crimson OSD Client Test" << std::endl;
        std::cout << "=============================" << std::endl;
        std::cout << "Connecting to OSD at: " << osd_addr << std::endl;
        std::cout << "This client tests basic connectivity and ping functionality with the OSD" << std::endl << std::endl;
        
        // Initialize Ceph configuration
        return crimson::common::sharded_conf().start(
            EntityName{}, std::string_view{"ceph"}
        ).then([] {
            return crimson::common::local_conf().start();
        }).then([osd_addr] {
            return run(osd_addr);
        }).then([] {
            return 0;
        }).handle_exception([](std::exception_ptr eptr) {
            try {
                std::rethrow_exception(eptr);
            } catch (const std::exception& e) {
                std::cerr << "Error: " << e.what() << std::endl;
            }
            return 1;
        }).finally([] {
            // Clean up configuration
            return crimson::common::sharded_conf().stop();
        });
    });
}
