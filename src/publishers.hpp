#ifndef REINFERIO_SALTFISH_PUBLISHERS_HPP
#define REINFERIO_SALTFISH_PUBLISHERS_HPP

#include "saltfish.pb.h"

#include <string>
#include <cstdint>


class redisContext;

namespace reinferio {
namespace saltfish {

class RedisPublisher {
 public:
  RedisPublisher(const std::string& host_ = "127.0.0.1",
                 const uint16_t port_ = 6379,
                 const std::string& key_ = "saltfish:pub");

  RedisPublisher(const RedisPublisher&) = delete;
  RedisPublisher& operator=(const RedisPublisher&) = delete;

  void publish(RequestType type, const std::string& msg);

  const std::string host;
  const uint16_t port;
  const std::string key;
 private:
  redisContext* context_;
};

}}  // namespace reinferio::saltfish

#endif  // REINFERIO_SALTFISH_SERVICE_HPP
