#ifndef REINFERIO_SALTFISH_SERVER_HPP
#define REINFERIO_SALTFISH_SERVER_HPP

#include "riak_proxy.hpp"
#include "service.hpp"

#include <boost/program_options.hpp>
#include <glog/logging.h>
#include <rpcz/rpcz.hpp>

#include <string>
#include <csignal>


namespace reinferio {
namespace saltfish {

class SaltfishServer {
 public:
  void run();
  void terminate();
  static std::shared_ptr<SaltfishServer> create_server(const std::string& bind_str,
                                                       const std::string& riak_host,
                                                       std::uint16_t riak_port);

 private:
  SaltfishServer(const std::string& bind_str,
                 const std::string& riak_host,
                 std::uint16_t riak_port);
  static void ctrlc_handler(int signum);

  std::string bind_str_;
  std::string riak_host_;
  std::uint16_t riak_port_;

  rpcz::application application_;
  rpcz::server server_;
  std::unique_ptr<RiakProxy> riak_proxy_;

  static std::shared_ptr<SaltfishServer> inst_;
};


}  // namespace reinferio
}  // namespace saltfish

#endif  // REINFERIO_SALTFISH_SERVICE_HPP
