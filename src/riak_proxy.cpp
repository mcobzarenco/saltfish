#include "riak_proxy.hpp"


namespace reinferio {
namespace saltfish {


void worker_thread(boost::asio::io_service* ios) {
  LOG(INFO) << "In thread " << std::this_thread::get_id();
  ios->run();
  LOG(INFO) << "Exiting thread " << std::this_thread::get_id();
}

std::shared_ptr<riak::object> random_sibling_resolution(const riak::siblings&) {
  std::cout << "Siblings being resolved!" << std::endl;
  auto new_content = std::make_shared<riak::object>();
  new_content->set_value("<result of sibling resolution>");
  return new_content;
}

RiakProxy::RiakProxy(const string& host, uint16_t port, uint8_t n_workers)
    :host_(host), port_(port), n_workers_(n_workers), ios_(),
     work_(new boost::asio::io_service::work(ios_)) {
  connect();
  init_threads();
}

void RiakProxy::get_object(const string& bucket, const string& key,
                           riak::get_response_handler handler) {
  client_->get_object(bucket, key, handler);
}


RiakProxy::~RiakProxy() {
  work_.reset();
  for (auto t = threads_.begin(); t != threads_.end(); ++t) {
    t->join();
  }
}


void RiakProxy::connect() {
  connection_ = riak::make_single_socket_transport(host_, port_, ios_);
  // this->connection = transport::make_zmq_riak_transport("tcp://localhost:5005");
  client_ = riak::make_client(connection_, &random_sibling_resolution, ios_);
}

void RiakProxy::init_threads() {
  for (uint8_t i = 0; i < n_workers_; ++i) {
    threads_.emplace_back(std::bind(worker_thread, &ios_));
  }
}




} // namespace reinferio
} // namespace saltfish
