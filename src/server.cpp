#include "server.hpp"

namespace reinferio {
namespace saltfish {

using namespace std;


shared_ptr<SaltfishServer> SaltfishServer::inst_ = nullptr;


shared_ptr<SaltfishServer> SaltfishServer::create_server(const std::string& bind_str,
                                                     const std::string& riak_host,
                                                     std::uint16_t riak_port) {
  if(SaltfishServer::inst_ == nullptr) {
    //SaltfishServer::inst_ = make_shared<SaltfishServer>(bind_str, riak_host, riak_port);
    SaltfishServer::inst_ = shared_ptr<SaltfishServer>(new SaltfishServer(bind_str, riak_host, riak_port));
  }
  return SaltfishServer::inst_;
}

SaltfishServer::SaltfishServer(const string& bind_str,
			       const string& riak_host,
			       uint16_t riak_port)
    :bind_str_(bind_str), riak_host_(riak_host), riak_port_(riak_port),
     application_(), server_(application_),
     riak_proxy_(new RiakProxy(riak_host, riak_port)) {
}

void SaltfishServer::run() {
  try {
    saltfish::SourceManagerService sms(riak_proxy_.get());
    server_.register_service(&sms);
    server_.bind(bind_str_);
    LOG(INFO) << "Serving requests at " << bind_str_
              << " (with Riak @ " << riak_host_ << ":" << riak_port_ << ")";
    signal(SIGINT, (void (*)(int)) &SaltfishServer::ctrlc_handler);
    application_.run();
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
  }
}

void SaltfishServer::terminate() {
}

void SaltfishServer::ctrlc_handler(int signum) {
  LOG(INFO) << "Interrupt signal (" << signum << ") received.";
  SaltfishServer::inst_->application_.terminate();
}


}  // namespace reinferio
}  // namespace saltfish
