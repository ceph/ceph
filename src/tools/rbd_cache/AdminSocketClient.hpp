#include <cstdio>
#include <iostream>
#include <array>
#include <memory>
#include <string>
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/algorithm/string.hpp>


using boost::asio::local::stream_protocol;

class client {
public:
  client(boost::asio::io_service& io_service, const std::string& file)
    : io_service_(io_service),
      socket_(io_service),
      ep_(stream_protocol::endpoint(file))
  {
  }

  int connect() {
    socket_.connect(ep_);
    return 0;
  }

  void handle_connect(const boost::system::error_code& error)
  {
    //TODO(): open librbd snap
  }

  void hand_write(const boost::system::error_code& error) {
  }

private:
  boost::asio::io_service& io_service_;
  stream_protocol::socket socket_;
  stream_protocol::endpoint ep_;
};


/*
int main(int argc, char* argv[]) {
  try {
    boost::asio::io_service io_service;

    //std::remove("/tmp/rbd_shared_readonly_cache_demo"); 
    client c(io_service, "/tmp/rbd_shared_readonly_cache_demo");

  } catch (std::exception& e) {
    std::cerr << "Exception: " << e.what() << "\n";
  }

  return 0;
}
*/
