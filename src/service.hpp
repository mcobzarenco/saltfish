#ifndef REINFERIO_SALTFISH_SERVICE_HPP
#define REINFERIO_SALTFISH_SERVICE_HPP

#include "service.pb.h"
#include "service.rpcz.h"

#include <rpcz/rpcz.hpp>

#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <riak/client.hxx>

#include <cstdint>
#include <string>
#include <memory>
#include <system_error>
#include <mutex>


namespace reinferio {
namespace saltfish {

class RiakProxy;

extern const uint32_t MAX_GENERATE_ID_COUNT;
extern const char SOURCES_METADATA_BUCKET[];
extern const char SOURCES_DATA_BUCKET_ROOT[];

typedef boost::uuids::uuid uuid_t;


// TODO(mcobzarenco): Error reporting member function (reply<>, error_code).
// With array from codes -> string.
class SourceManagerServiceImpl : public SourceManagerService {
 public:
  SourceManagerServiceImpl(RiakProxy* riak_proxy);
  virtual void create_source(const CreateSourceRequest& request,
                             rpcz::reply<CreateSourceResponse> reply) override;
  virtual void delete_source(const DeleteSourceRequest& request,
                             rpcz::reply<DeleteSourceResponse> reply) override;
  virtual void generate_id(const GenerateIdRequest& request,
                           rpcz::reply<GenerateIdResponse> reply) override;
  virtual void put_records(const PutRecordsRequest& request,
                           rpcz::reply<PutRecordsResponse> reply) override;


 private:
  uuid_t generate_uuid();
  void put_records_check_handler(const PutRecordsRequest& request,
                                 rpcz::reply<PutRecordsResponse> reply,
                                 const std::error_code& error,
                                 std::shared_ptr<riak::object> object,
                                 riak::value_updater& update_value);

  RiakProxy* riak_proxy_;
  boost::uuids::random_generator uuid_generator_;
  std::mutex uuid_generator_mutex_;
};


}  // namespace saltfish
}  // namespace reinferio

#endif  // REINFERIO_SALTFISH_SERVICE_HPP
