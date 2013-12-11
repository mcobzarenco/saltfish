#ifndef REINFERIO_SALTFISH_SERVER_HPP
#define REINFERIO_SALTFISH_SERVER_HPP

#include "riak_proxy.hpp"
#include "sql_pool.hpp"

#include "config.pb.h"

#include <boost/asio.hpp>
#include <glog/logging.h>
#include <rpcz/rpcz.hpp>

#include <cstdint>
#include <string>
#include <thread>


namespace reinferio {
namespace saltfish {

class SaltfishServer {
 public:
  SaltfishServer(const config::Saltfish& config);
  virtual ~SaltfishServer() noexcept;

  SaltfishServer(const SaltfishServer&) = delete;
  SaltfishServer& operator=(const SaltfishServer&) = delete;

  void run() noexcept;
  void terminate() noexcept;

  const config::Saltfish& get_config() const noexcept { return config_; }

 private:
  void ctrlc_handler(const boost::system::error_code& error, int signum) noexcept;

  config::Saltfish config_;

  boost::asio::io_service signal_ios_;
  std::unique_ptr<std::thread> signal_thread_;

  rpcz::application application_;
  rpcz::server server_;
  RiakProxy riak_proxy_;
  sql::ConnectionPool sql_pool_;
};


}  // namespace saltfish
}  // namespace reinferio

#endif  // REINFERIO_SALTFISH_SERVER_HPP
