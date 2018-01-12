#include <cstdio>
#include <iostream>
#include <array>
#include <memory>
#include <string>
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/algorithm/string.hpp>


using boost::asio::local::stream_protocol;

class session : public std::enable_shared_from_this<session> {
public:
  session(boost::asio::io_service& io_service) : socket_(io_service) {}

  stream_protocol::socket& socket() {
    return socket_;
  }

  void start() {
    socket_.async_read_some(boost::asio::buffer(data_),
        boost::bind(&session::handle_read,
          shared_from_this(),
          boost::asio::placeholders::error,
          boost::asio::placeholders::bytes_transferred));
  }

  void handle_read(const boost::system::error_code& error, size_t bytes_transferred) {
    //TODO(): parse message, format=iotype rbdname offset length

    //TODO(): return content, do promotion if needed
    if (!error) {
      std::string raw_cmd_str(data_.data());
      std::vector<std::string> cmd_str_vector;
      boost::split(cmd_str_vector, raw_cmd_str, boost::is_any_of(" "));

      assert(cmd_str_vector.size() == 4);
      std::string io_type = cmd_str_vector[0];
      std::string rbd_name = cmd_str_vector[1];
      std::string rbd_offset = cmd_str_vector[2];
      std::string rbd_length = cmd_str_vector[3];
      std::cout << "AAAA " << io_type << std::endl;
      
      boost::asio::async_write(socket_,
          boost::asio::buffer(data_, bytes_transferred),
          boost::bind(&session::handle_write,
            shared_from_this(),
            boost::asio::placeholders::error));
    }
  }

  void handle_write(const boost::system::error_code& error) {
    if (!error) {
      socket_.async_read_some(boost::asio::buffer(data_),
          boost::bind(&session::handle_read,
            shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred));
    }
  }

private:
  // The socket used to communicate with the client.
  stream_protocol::socket socket_;

  // Buffer used to store data received from the client.
  std::array<char, 1024> data_;
};

typedef std::shared_ptr<session> session_ptr;

class server {
public:
  server(boost::asio::io_service& io_service, const std::string& file)
    : io_service_(io_service),
      acceptor_(io_service, stream_protocol::endpoint(file))
  {
    session_ptr new_session(new session(io_service_));
    acceptor_.async_accept(new_session->socket(),
        boost::bind(&server::handle_accept, this, new_session,
          boost::asio::placeholders::error));
  }

  void handle_accept(session_ptr new_session,
      const boost::system::error_code& error)
  {
    //TODO(): open librbd snap
    if (!error) {
      new_session->start();
      new_session.reset(new session(io_service_));
      acceptor_.async_accept(new_session->socket(),
          boost::bind(&server::handle_accept, this, new_session,
            boost::asio::placeholders::error));
    }
  }

private:
  boost::asio::io_service& io_service_;
  stream_protocol::acceptor acceptor_;
};

/*
int main(int argc, char* argv[]) {
  try {
    boost::asio::io_service io_service;

    std::remove("/tmp/rbd_shared_readonly_cache_demo"); 
    server s(io_service, "/tmp/rbd_shared_readonly_cache_demo");

    io_service.run();
  } catch (std::exception& e) {
    std::cerr << "Exception: " << e.what() << "\n";
  }

  return 0;
}
*/
