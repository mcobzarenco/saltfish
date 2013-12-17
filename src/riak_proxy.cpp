#include "riak_proxy.hpp"

#include <glog/logging.h>


namespace reinferio {
namespace saltfish {

using namespace std;


void worker_thread(boost::asio::io_service* ios) {
  LOG(INFO) << "Calling io_service::run() in thread " << std::this_thread::get_id();
  ios->run();
  LOG(INFO) << "Exiting thread " << std::this_thread::get_id();
}

// TODO: Actually handle sibling resolution.
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
  // LOG(INFO) << "Queueing Riak get_object request";
  client_->get_object(bucket, key, handler);
}

void RiakProxy::delete_object(const string& bucket, const string& key,
			      riak::delete_response_handler handler) {
  LOG(INFO) << "Queueing Riak delete_object request";
  client_->delete_object(bucket, key, handler);
}

RiakProxy::~RiakProxy() {
}

void RiakProxy::connect() {
  connection_ = riak::make_single_socket_transport(host_, port_, ios_);
  // this->connection = transport::make_zmq_riak_transport("tcp://localhost:5005");
  client_ = riak::make_client(connection_, &random_sibling_resolution, ios_);
}

void RiakProxy::init_threads() {
  LOG(INFO) << "Spawning " << n_workers_ << " worker threads in Riak proxy";
  for (uint8_t i = 0; i < n_workers_; ++i) {
    threads_.emplace_back(std::bind(worker_thread, &ios_));
  }
}


} // namespace reinferio
} // namespace saltfish
