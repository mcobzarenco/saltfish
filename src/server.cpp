#define BOOST_BIND_NO_PLACEHOLDERS

#include <riakpp/connection_pool.hpp>
#include "server.hpp"
#include "service.hpp"
#include "publishers.hpp"

#include "service.pb.h"


namespace reinferio {
namespace saltfish {

using namespace std;
using namespace std::placeholders;

SaltfishServer::SaltfishServer(const config::Saltfish& config)
    : config_(config), signal_ios_(), signal_thread_(),
      ios_(), work_(new boost::asio::io_service::work(ios_)), context_(1),
      application_(), server_(application_),
      riak_client_(config.riak().host(), config.riak().port()),
      sql_store_(context_, config.maria_db().host(),
                 static_cast<uint16_t>(config.maria_db().port()),
                 config.maria_db().user(), config.maria_db().password(),
                 config.maria_db().db()),
      redis_pub_(config.redis().host(), config.redis().port(),
                 config.redis().key()) {
  for(int i = 0; i < 5; ++i) {
    threads_.emplace_back( ([this]() { this->ios_.run(); }) );
  }
}

SaltfishServer::~SaltfishServer() noexcept {
  if (signal_thread_ != nullptr) {
    signal_ios_.stop();
    signal_thread_->join();
  }
  work_.reset();
  ios_.stop();
  for (auto t = threads_.begin(); t != threads_.end(); ++t) {
    t->join();
  }
}

void SaltfishServer::run() noexcept {
  try {
    saltfish::SaltfishServiceImpl saltfish_serv(
        riak_client_, sql_store_, ios_,
        config_.max_generate_id_count(),
        config_.sources_data_bucket_prefix());
    auto listener = bind(&RedisPublisher::publish, &redis_pub_, _1, _2);
    saltfish_serv.register_listener(RequestType::ALL, listener);

    // LOG(INFO) << "Exchange: " << rabbit_pub_.exchange;
    server_.register_service(&saltfish_serv);
    server_.bind(config_.bind_str());
    LOG(INFO) << "Serving requests at " << config_.bind_str() << " (riak at "
              << config_.riak().host() << ":" << config_.riak().port() << "; "
              << config_.maria_db().user() << "@mariadb/"
              << config_.maria_db().db() << " at " << config_.maria_db().host()
              << ":" << config_.maria_db().port() << "; "
              << "redis at " << config_.redis().host() << ":"
              << config_.redis().port() << ")";

    boost::asio::signal_set signals(signal_ios_, SIGINT, SIGTERM);
    auto signal_handler = std::bind(&SaltfishServer::ctrlc_handler, this, _1, _2);
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
  LOG(INFO) << "Interrupt signal " << signum << " received;"
            << " error_code=" << error;
  terminate();
}

}}  // namespace reinferio::saltfish
