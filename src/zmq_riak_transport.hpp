#ifndef ZMQ_RIAK_TRANSPORT_HPP
#define ZMQ_RIAK_TRANSPORT_HPP


#include <riak/transport.hxx>


namespace reinferio {
namespace saltfish {
namespace transport {

    /*!
     * Produces a transport providing serial delivery of requests along one socket at a time. All of
     * these requests will act under the given client_id.
     */
    riak::transport::delivery_provider make_zmq_riak_transport(const std::string& broker_address);

}   // namespace transport
}   // namespace saltfish
}


#endif  // ZMQ_RIAK_TRANSPORT_HPP
