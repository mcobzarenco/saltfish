#ifndef REINFERIO_SALTFISH_SERVICE_HPP
#define REINFERIO_SALTFISH_SERVICE_HPP

#include "sql.hpp"
#include "reinferio/saltfish.pb.h"
#include "reinferio/saltfish.rpcz.h"

#include <rpcz/rpcz.hpp>
#include <riakpp/client.hpp>
#include <boost/asio.hpp>

#include <cstdint>
#include <memory>
#include <system_error>
#include <string>
#include <vector>


namespace riak {
class client;
}

namespace reinferio { namespace saltfish {

class DatasetStoreImpl : public DatasetStore {
 public:
    using Listener = std::function<void(RequestType req_type,
                                        const std::string&)>;
  DatasetStoreImpl(
      riak::client& riak_client,
      store::MetadataSqlStoreTasklet& sql_store,
      boost::asio::io_service& ios,
      uint32_t max_generate_id_count,
      const std::string& records_bucket_prefix,
      const std::string& schemas_bucket,
      const uint64_t max_random_index);

  DatasetStoreImpl(const DatasetStoreImpl&) = delete;
  DatasetStoreImpl& operator=(const DatasetStoreImpl&) = delete;

  virtual void create_dataset(const CreateDatasetRequest& request,
                             rpcz::reply<CreateDatasetResponse> reply) override;
  virtual void delete_dataset(const DeleteDatasetRequest& request,
                             rpcz::reply<DeleteDatasetResponse> reply) override;
  virtual void generate_id(const GenerateIdRequest& request,
                           rpcz::reply<GenerateIdResponse> reply) override;
  virtual void get_datasets(const GetDatasetsRequest& request,
                            rpcz::reply<GetDatasetsResponse> reply) override;
  virtual void put_records(const PutRecordsRequest& request,
                           rpcz::reply<PutRecordsResponse> reply) override;

  virtual void register_listener(RequestType req_type,
                                 const Listener& listener) {
    listeners_.emplace_back(req_type, listener, ios_);
  }
 private:
  class ListenerInfo {
   public:
    ListenerInfo(RequestType req_type, const Listener& hdl,
                 boost::asio::io_service& ios)
        : listens_to(req_type), handler(hdl), strand(ios) {}

    RequestType listens_to;
    Listener handler;
    boost::asio::io_service::strand strand;
  };
  inline void async_call_listeners(
      RequestType req_type, const std::string& request);
  inline std::vector<std::string> ids_for_put_request(
      const PutRecordsRequest& request);

  riak::client& riak_client_;
  store::MetadataSqlStoreTasklet& sql_store_;
  boost::asio::io_service& ios_;

  uint32_t max_generate_id_count_;
  const std::string records_bucket_prefix_;
  const std::string schemas_bucket_;
  const uint64_t max_random_index_;

  std::vector<ListenerInfo> listeners_;
};

}}  // namespace reinferio::saltfish

#endif  // REINFERIO_SALTFISH_SERVICE_HPP
