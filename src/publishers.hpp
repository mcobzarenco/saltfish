#ifndef REINFERIO_SALTFISH_PUBLISHERS_HPP
#define REINFERIO_SALTFISH_PUBLISHERS_HPP

#include "service.pb.h"

#include <SimpleAmqpClient/SimpleAmqpClient.h>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>

#include <string>
#include <cstdint>


namespace reinferio {
namespace saltfish {

class RabbitPublisher {
 public:
  RabbitPublisher(const std::string& host_ = "127.0.0.1",
                  const uint16_t port_ = 5672,
                  const std::string& username_ = "guest",
                  const std::string& password_ = "guest",
                  const std::string& exchange_ = "saltfish");

  RabbitPublisher(const RabbitPublisher&) = delete;
  RabbitPublisher& operator=(const RabbitPublisher&) = delete;

  void publish(RequestType type, const std::string& msg);

  const std::string host;
  const uint16_t port;
  const std::string username;
  const std::string password;

  const std::string exchange;
 private:
  AmqpClient::Channel::ptr_t channel_;
};

}}  // namespace reinferio::saltfish

#endif  // REINFERIO_SALTFISH_SERVICE_HPP
