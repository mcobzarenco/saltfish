#ifndef REINFERIO_SALTFISH_SERVER_HPP
#define REINFERIO_SALTFISH_SERVER_HPP

#include "riak_proxy.hpp"
#include "service.hpp"

#include <boost/asio.hpp>
#include <glog/logging.h>
#include <rpcz/rpcz.hpp>

#include <string>


namespace reinferio {
namespace saltfish {

class SaltfishServer {
 public:
  SaltfishServer(const std::string& bind_str,
                 const std::string& riak_host,
                 std::uint16_t riak_port);
  virtual ~SaltfishServer();

  void run();
  void terminate();

  std::string bind_str() const {return bind_str_;}
  std::string riak_host() const {return riak_host_;}
  std::uint16_t riak_port() const {return riak_port_;}

 private:
  void ctrlc_handler(const boost::system::error_code& error, int signum);

  std::string bind_str_;
  std::string riak_host_;
  std::uint16_t riak_port_;

  boost::asio::io_service signal_ios_;
  std::unique_ptr<std::thread> signal_thread_;

  rpcz::application application_;
  rpcz::server server_;
  std::unique_ptr<RiakProxy> riak_proxy_;
};


}  // namespace saltfish
}  // namespace reinferio

#endif  // REINFERIO_SALTFISH_SERVICE_HPP
