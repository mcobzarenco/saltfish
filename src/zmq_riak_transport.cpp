#include <iostream>
#include <riak/transports/single_serial_socket.hxx>
#include <zmq.hpp>
#include <system_error>

#include "zmq_riak_transport.hpp"


namespace reinferio {
namespace saltfish {
namespace transport {


class zmq_riak_transport : public std::enable_shared_from_this<zmq_riak_transport> {
  public:
    zmq_riak_transport(const std::string& broker_address);
    // virtual ~zmq_riak_transport();

    virtual riak::transport::option_to_terminate_request deliver (
            const std::string& r,
            riak::transport::response_handler h);

  private:
    std::string broker_address_;
    std::shared_ptr<zmq::context_t> context_;
    zmq::socket_t socket_;
};



using std::placeholders::_1;
using std::placeholders::_2;


riak::transport::delivery_provider make_zmq_riak_transport(const std::string& broker_address)
{
    auto transport = std::make_shared<zmq_riak_transport>(broker_address);
    return std::bind(&zmq_riak_transport::deliver, transport, _1, _2);
}


// zmq_riak_transport implementation:

zmq_riak_transport::zmq_riak_transport(const std::string& broker_address)
    :broker_address_(broker_address), context_(new zmq::context_t(1)),
     socket_(*context_, ZMQ_REQ) {
    socket_.connect(broker_address.c_str());
}


void terminate_req(bool) {
    return;
}


riak::transport::option_to_terminate_request zmq_riak_transport::deliver (
					     const std::string& r,
					     riak::transport::response_handler h) {
    zmq::message_t request (6);
    memcpy ((void *) request.data (), "Hello", 6);
    std::cout << "Sending Hello ..." << std::endl;
    socket_.send (request);

    //  Get the reply.
    zmq::message_t reply;
    socket_.recv (&reply);
    std::cout << "Received World " << std::endl;
    return &terminate_req;
}



}   // namespace transport
}   // namespace saltfish
}
