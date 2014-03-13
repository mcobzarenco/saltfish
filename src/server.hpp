#ifndef REINFERIO_SALTFISH_SERVER_HPP
#define REINFERIO_SALTFISH_SERVER_HPP

#include "sql_pool.hpp"
#include "publishers.hpp"
#include "config.pb.h"

#include <riakpp/client.hpp>
#include <rpcz/rpcz.hpp>
#include <glog/logging.h>
#include <zmq.hpp>
#include <boost/asio.hpp>

#include <cstdint>
#include <memory>
#include <string>
#include <thread>


namespace reinferio { namespace saltfish {

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

  boost::asio::io_service ios_;
  std::unique_ptr<boost::asio::io_service::work> work_;
  std::vector<std::thread> threads_;

  zmq::context_t context_;
  rpcz::application application_;
  rpcz::server server_;
  riak::client riak_client_;
  store::SourceMetadataSqlStoreTasklet sql_store_;
  RedisPublisher redis_pub_;
};

}}  // namespace saltfish::reinferio

#endif  // REINFERIO_SALTFISH_SERVER_HPP
