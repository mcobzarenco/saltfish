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
      ios_(), work_(new boost::asio::io_service::work(ios_)),
      application_(), server_(application_),
      //      riak_client_(config.riak().host(), config.riak().port(), ios_),
      riak_client_(config.riak().host(), config.riak().port()),
      // sql_factory_(config.maria_db().host(), config.maria_db().user(),
      //              config.maria_db().password(), config.maria_db().db()),
      sql_factory_(unique_ptr<sql::ConnectionFactory>{new sql::ConnectionFactory{config.maria_db().host(), config.maria_db().user(), config.maria_db().password(), config.maria_db().db()}}),
      rabbit_pub_(config.rabbit_mq().host(), config.rabbit_mq().port(),
                  config.rabbit_mq().user(), config.rabbit_mq().password()) {
  for(int i = 0; i < 5; ++i) {
    threads_.emplace_back( ([this]() {
          LOG(INFO) << "Calling io_service::run() in thread "
                    << std::this_thread::get_id();
          this->ios_.run();
          LOG(INFO) << "Exiting thread " << std::this_thread::get_id();
        }) );
  }
}

SaltfishServer::~SaltfishServer() noexcept {
  sql_factory_.reset();
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
        riak_client_, *sql_factory_, ios_,
        config_.max_generate_id_count(),
        config_.sources_data_bucket_prefix());
    auto rmq_listener = bind(&RabbitPublisher::publish, &rabbit_pub_, _1, _2);
    saltfish_serv.register_listener(RequestType::ALL, rmq_listener);

    // LOG(INFO) << "Exchange: " << rabbit_pub_.exchange;
    server_.register_service(&saltfish_serv);
    server_.bind(config_.bind_str());
    LOG(INFO) << "Serving requests at " << config_.bind_str() << " (with Riak @ "
              << config_.riak().host() << ":" << config_.riak().port() << ")";

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
