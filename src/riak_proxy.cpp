#include "riak_proxy.hpp"

#include <glog/logging.h>


namespace reinferio {
namespace saltfish {

using namespace std;


// TODO: Actually handle sibling resolution.
std::shared_ptr<riak::object> random_sibling_resolution(const riak::siblings&) {
  std::cout << "Siblings being resolved!" << std::endl;
  auto new_content = std::make_shared<riak::object>();
  new_content->set_value("<result of sibling resolution>");
  return new_content;
}

RiakProxy::RiakProxy(const string& host, uint16_t port,
                     boost::asio::io_service& ios)
    :host_(host), port_(port), ios_(ios) {
    connect();
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


} // namespace reinferio
} // namespace saltfish
