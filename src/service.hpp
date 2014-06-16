#ifndef REINFERIO_SALTFISH_SERVICE_HPP
#define REINFERIO_SALTFISH_SERVICE_HPP

#include "sql.hpp"
#include "record_summarizer.hpp"
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

class SaltfishService : public SourceManagerService {
 public:
  using Listener = std::function<void(RequestType req_type,
                                      const std::string&)>;

  virtual void register_listener(RequestType req_type,
                                 const Listener& listener) = 0;
};

class SaltfishServiceImpl : public SaltfishService {
 public:
  SaltfishServiceImpl(
      riak::client& riak_client,
      store::MetadataSqlStoreTasklet& sql_store,
      boost::asio::io_service& ios,
      uint32_t max_generate_id_count,
      const std::string& sources_data_bucket_prefix,
      const std::string& schemas_bucket);

  SaltfishServiceImpl(const SaltfishServiceImpl&) = delete;
  SaltfishServiceImpl& operator=(const SaltfishServiceImpl&) = delete;

  virtual void create_source(const CreateSourceRequest& request,
                             rpcz::reply<CreateSourceResponse> reply) override;
  virtual void delete_source(const DeleteSourceRequest& request,
                             rpcz::reply<DeleteSourceResponse> reply) override;
  virtual void generate_id(const GenerateIdRequest& request,
                           rpcz::reply<GenerateIdResponse> reply) override;
  virtual void get_sources(const GetSourcesRequest& request,
                            rpcz::reply<GetSourcesResponse> reply) override;
  virtual void put_records(const PutRecordsRequest& request,
                           rpcz::reply<PutRecordsResponse> reply) override;

  virtual void register_listener(RequestType req_type,
                                 const Listener& listener) override {
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
  const std::string sources_data_bucket_prefix_;
  const std::string schemas_bucket_;

  std::vector<ListenerInfo> listeners_;

  SummarizerMap summarizer_map_;
};

}}  // namespace reinferio::saltfish

#endif  // REINFERIO_SALTFISH_SERVICE_HPP
