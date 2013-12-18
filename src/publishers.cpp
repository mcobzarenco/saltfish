#include "publishers.hpp"

#include <glog/logging.h>


using namespace std;
using namespace AmqpClient;

namespace reinferio {
namespace saltfish {

RabbitPublisher::RabbitPublisher(
    const string& host_, const uint16_t port_,
    const string& username_, const string& password_,
    const string& exchange_)
    : host(host_), port(port_), username(username_),
      password(password_), exchange(exchange_)  {
  channel_ = Channel::Create(host_, port_, username_, password_);
  channel_->DeclareExchange(exchange_, Channel::EXCHANGE_TYPE_FANOUT);
}

void RabbitPublisher::publish(RequestType type, const string& msg) {
  LOG(INFO) << "publishing msg on RMQ";
  BasicMessage::ptr_t msg_in = BasicMessage::Create();
  msg_in->Body(msg);
  channel_->BasicPublish(exchange, "", msg_in);
}


}}  // namespace reinferio::saltfish
