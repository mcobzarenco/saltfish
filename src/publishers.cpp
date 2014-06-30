#include "publishers.hpp"

#include <glog/logging.h>
#include <hiredis/hiredis.h>

#include <memory>


using namespace std;

namespace reinferio {
namespace saltfish {

RedisPublisher::RedisPublisher(
    const string& host_, const uint16_t port_, const string& key_)
    : host(host_), port(port_), key(key_) {
  timeval timeout{1, 500000};
  context_ = redisConnectWithTimeout(host.c_str(), port, timeout);
}

void RedisPublisher::publish(RequestType type, const string& msg) {
  // TODO(cristicbz): Publish more request types once we overhaul listener
  // system.
  if (type != PUT_RECORDS) return;

  LOG(INFO) << "Publishing msg on Redis " << type << " " << PUT_RECORDS;
  auto del = [](redisReply* r) {
    LOG(INFO) << "freeing redis reply";
    freeReplyObject(r);
  };
  unique_ptr<redisReply, decltype(del)> reply{static_cast<redisReply *>(
  redisCommand(context_, "PUBLISH %b %b",
               key.c_str(), key.size(), msg.c_str(), msg.size())), del};
  LOG(INFO) << "Redis reply: " << reply->str;
}

}}  // namespace reinferio::saltfish
