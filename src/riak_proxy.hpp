#ifndef REINFERIO_SALTFISH_RIAK_PROXY_HPP
#define REINFERIO_SALTFISH_RIAK_PROXY_HPP

#include <boost/asio.hpp>
#include <riak/client.hxx>
#include <riak/response_handlers.hxx>
#include <riak/transports/single_serial_socket.hxx>

#include <cstdint>
#include <string>
#include <thread>


namespace reinferio {
namespace saltfish {

typedef std::shared_ptr<riak::client> client_ptr;

class RiakProxy {
 public:
  RiakProxy(const std::string& host, uint16_t port,
            boost::asio::io_service& ios);
  RiakProxy(const RiakProxy&) = delete;
  RiakProxy& operator=(const RiakProxy&) = delete;
  ~RiakProxy();

  void get_object(const std::string& bucket, const std::string& key,
                  riak::get_response_handler);
  void delete_object(const std::string& bucket, const std::string& key,
                     riak::delete_response_handler);

 private:
  void connect();

  const std::string host_;
  const uint16_t port_;

  boost::asio::io_service& ios_;
  riak::transport::delivery_provider connection_;
  client_ptr client_;
};


std::shared_ptr<riak::object> random_sibling_resolution(const riak::siblings&);



}  // namespace reinferio
}  // namespace saltfish


#endif  // REINFERIO_SALTFISH_RIAK_PROXY_HPP
