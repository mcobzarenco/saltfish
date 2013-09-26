#ifndef RIO_SALTFISH_RIAK_PROXY_HPP
#define RIO_SALTFISH_RIAK_PROXY_HPP

#include <string>

#include <riak/client.hxx>
#include <riak/response_handlers.hxx>
#include <riak/transports/single_serial_socket.hxx>


namespace rio {
namespace saltfish {


typedef std::shared_ptr<riak::client> store_ptr;

using namespace std::placeholders;
using std::string;


class RiakProxy {
public:
    RiakProxy(const string& host, int port);
    void get_object (const string& bucket, const string& k, riak::get_response_handler);
private:

    boost::asio::io_service ios;
    riak::transport::delivery_provider connection;
    store_ptr store;
};


std::shared_ptr<riak::object> random_sibling_resolution(const riak::siblings&);
void print_object_value(const std::error_code& error,
			std::shared_ptr<riak::object> object,
			riak::value_updater& update_value);




} // namespace rio
} // namespace saltfish

#endif  // RIO_SALTFISH_RIAK_PROXY_HPP
