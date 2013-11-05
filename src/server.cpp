#include "server.hpp"

namespace reinferio {
namespace saltfish {

using namespace std;


SaltfishServer::SaltfishServer(const string& bind_str,
			       const string& riak_host,
			       uint16_t riak_port)
    : bind_str_(bind_str), riak_host_(riak_host), riak_port_(riak_port),
      signal_ios_(), signal_thread_(), application_(), server_(application_) {
  riak_proxy_.reset(new RiakProxy(riak_host, riak_port));
}

SaltfishServer::~SaltfishServer() {
  if (signal_thread_ != nullptr) {
    signal_ios_.stop();
    signal_thread_->join();
  }
}

void SaltfishServer::run() noexcept {
  try {
    saltfish::SourceManagerService sms(riak_proxy_.get());
    server_.register_service(&sms);
    server_.bind(bind_str_);
    LOG(INFO) << "Serving requests at " << bind_str_
              << " (with Riak @ " << riak_host_ << ":" << riak_port_ << ")";

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
