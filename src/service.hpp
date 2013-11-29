#ifndef REINFERIO_SALTFISH_SERVICE_HPP
#define REINFERIO_SALTFISH_SERVICE_HPP

#include "service.pb.h"
#include "service.rpcz.h"

#include <rpcz/rpcz.hpp>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <riak/client.hxx>

#include <cstdint>
#include <string>
#include <memory>
#include <system_error>
#include <mutex>


namespace reinferio {
namespace saltfish {

class RiakProxy;

namespace sql {
class ConnectionPool;
}


using uuid_t = boost::uuids::uuid;

class SourceManagerServiceImpl : public SourceManagerService {
 public:
  SourceManagerServiceImpl(
      RiakProxy& riak_proxy,
      uint32_t max_generate_id_count,
      const std::string& sources_data_bucket_root,
      const std::string& sources_metadata_bucket = "/ml/sources/schemas/");

  virtual void create_source(const CreateSourceRequest& request,
                             rpcz::reply<CreateSourceResponse> reply) override;
  virtual void delete_source(const DeleteSourceRequest& request,
                             rpcz::reply<DeleteSourceResponse> reply) override;
  virtual void generate_id(const GenerateIdRequest& request,
                           rpcz::reply<GenerateIdResponse> reply) override;
  virtual void put_records(const PutRecordsRequest& request,
                           rpcz::reply<PutRecordsResponse> reply) override;

 private:
  inline int64_t generate_random_index();
  inline uuid_t generate_uuid();

  void create_source_handler(const std::string& source_id,
                             const CreateSourceRequest& request,
                             rpcz::reply<CreateSourceResponse> reply,
                             const std::error_code& error,
                             std::shared_ptr<riak::object> object,
                             riak::value_updater& update_value);
  void put_records_check_handler(const PutRecordsRequest& request,
                                 rpcz::reply<PutRecordsResponse> reply,
                                 const std::error_code& error,
                                 std::shared_ptr<riak::object> object,
                                 riak::value_updater& update_value);

  RiakProxy& riak_proxy_;
  // sql::ConnectionPool sql_pool_;

  boost::uuids::random_generator uuid_generator_;
  std::mutex uuid_generator_mutex_;
  std::function<int64_t()> uniform_distribution_;
  std::mutex uniform_distribution_mutex_;

  uint32_t max_generate_id_count_;
  const std::string sources_metadata_bucket_;
  const std::string sources_data_bucket_root_;
};


}  // namespace saltfish
}  // namespace reinferio

#endif  // REINFERIO_SALTFISH_SERVICE_HPP
