#ifndef REINFERIO_SALTFISH_RIAK_PROXY_HPP
#define REINFERIO_SALTFISH_RIAK_PROXY_HPP

#include <boost/asio.hpp>
#include <glog/logging.h>
#include <riak/client.hxx>
#include <riak/response_handlers.hxx>
#include <riak/transports/single_serial_socket.hxx>

#include <string>
#include <thread>


namespace reinferio {
namespace saltfish {


typedef std::shared_ptr<riak::client> client_ptr;

using namespace std::placeholders;
using std::string;


class RiakProxy {
 public:
  RiakProxy(const string& host, uint16_t port, uint8_t n_workers=3);
  RiakProxy(const RiakProxy&) = delete;
  RiakProxy& operator=(const RiakProxy&) = delete;
  virtual ~RiakProxy();

  void get_object(const string& bucket, const string& key,
                  riak::get_response_handler);
  void delete_object(const string& bucket, const string& key,
                     riak::delete_response_handler);

 private:
  void connect();
  void init_threads();

  const string host_;
  const uint16_t port_;
  const uint16_t n_workers_;

  boost::asio::io_service ios_;
  std::unique_ptr<boost::asio::io_service::work> work_;
  std::vector<std::thread> threads_;
  riak::transport::delivery_provider connection_;
  client_ptr client_;
};


std::shared_ptr<riak::object> random_sibling_resolution(const riak::siblings&);



}  // namespace reinferio
}  // namespace saltfish


#endif  // REINFERIO_SALTFISH_RIAK_PROXY_HPP
