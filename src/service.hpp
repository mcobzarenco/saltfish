#ifndef REINFERIO_SALTFISH_SERVICE_HPP
#define REINFERIO_SALTFISH_SERVICE_HPP

#include "service.pb.h"
#include "service.rpcz.h"
#include "riak_proxy.hpp"

#include <rpcz/rpcz.hpp>

#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.

#include <thread>


namespace reinferio {
namespace saltfish {

typedef boost::uuids::uuid uuid_t;


class SourceManagerService : public SourceManager {
 public:
  SourceManagerService(RiakProxy* riak_proxy);
  virtual void push_rows(const Request& request, rpcz::reply<saltfish::Response> response) override;

 private:
  RiakProxy* riak_proxy;
  boost::uuids::random_generator uuid_generator;

};


}  // namespace reinferio
}  // namespace saltfish

#endif  // REINFERIO_SALTFISH_SERVICE_HPP
