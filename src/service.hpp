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

const uint32_t MAX_GENERATE_ID_COUNT{1000};
const string SOURCES_META_BUCKET{"/ml/sources/schemas/"};
const string SOURCES_DATA_BUCKET{"/ml/sources/data/"};


class SourceManagerService : public SourceManager {
 public:
  SourceManagerService(RiakProxy* riak_proxy);
  virtual void create_source(const CreateSourceRequest& request,
                             rpcz::reply<CreateSourceResponse> reply) override;
  virtual void delete_source(const DeleteSourceRequest& request,
                             rpcz::reply<DeleteSourceResponse> reply) override;
  virtual void generate_id(const GenerateIdRequest& request,
                           rpcz::reply<GenerateIdResponse> reply) override;
  virtual void put_records(const PutRecordsRequest& request,
			   rpcz::reply<PutRecordsResponse> reply) override;


 private:
  RiakProxy* riak_proxy_;
  boost::uuids::random_generator uuid_generator_;

};


}  // namespace reinferio
}  // namespace saltfish

#endif  // REINFERIO_SALTFISH_SERVICE_HPP
