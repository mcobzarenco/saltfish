#include "server.hpp"
#include "service.hpp"

#include <mysql++/mysql++.h>


namespace reinferio {
namespace saltfish {

using namespace std;

SaltfishServer::SaltfishServer(const SaltfishConf& config)
    : config_(config), signal_ios_(), signal_thread_(),
      application_(), server_(application_),
      riak_proxy_(config.riak().host(), config.riak().port()),
      sql_pool_(config.maria_db().host(), config.maria_db().db(),
                config.maria_db().user(), config.maria_db().password()) {
}

SaltfishServer::~SaltfishServer() noexcept {
  if (signal_thread_ != nullptr) {
    signal_ios_.stop();
    signal_thread_->join();
  }
}

void SaltfishServer::run() noexcept {
  try {
    saltfish::SourceManagerServiceImpl sms(riak_proxy_,
                                           sql_pool_,
                                           config_.max_generate_id_count(),
                                           config_.sources_data_bucket_root());
    server_.register_service(&sms);
    server_.bind(config_.bind_str());
    LOG(INFO) << "Serving requests at " << config_.bind_str() << " (with Riak @ "
              << config_.riak().host() << ":" << config_.riak().port() << ")";

    boost::asio::signal_set signals(signal_ios_, SIGINT, SIGTERM);
    auto signal_handler = std::bind(&SaltfishServer::ctrlc_handler, this,
                                    placeholders::_1, placeholders::_2);
    signals.async_wait(signal_handler);
    signal_thread_.reset(new std::thread([&]() { signal_ios_.run(); }));

    application_.run();
    LOG(INFO) << "Stopping the server...";
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
  }
}

void SaltfishServer::terminate() noexcept {
  application_.terminate();
}

void SaltfishServer::ctrlc_handler(
    const boost::system::error_code& error,
    int signum) noexcept {
  LOG(INFO) << "Interrupt signal (" << signum << ") received.";
  terminate();
}


}  // namespace saltfish
}  // namespace reinferio
