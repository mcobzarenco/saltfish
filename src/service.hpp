#ifndef REINFERIO_SALTFISH_SERVICE_HPP
#define REINFERIO_SALTFISH_SERVICE_HPP

#include "sql_pool.hpp"

#include "service.pb.h"
#include "service.rpcz.h"

#include <rpcz/rpcz.hpp>
#include <riak/client.hxx>
#include <boost/asio.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>

#include <cstdint>
#include <string>
#include <memory>
#include <system_error>
#include <mutex>
#include <functional>
#include <vector>


namespace reinferio {
namespace saltfish {

class RiakProxy;

using uuid_t = boost::uuids::uuid;

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
      RiakProxy& riak_proxy,
      sql::ConnectionFactory& sql_factory,
      boost::asio::io_service& ios,
      uint32_t max_generate_id_count,
      const std::string& sources_data_bucket_prefix);

  SaltfishServiceImpl(const SaltfishServiceImpl&) = delete;
  SaltfishServiceImpl& operator=(const SaltfishServiceImpl&) = delete;

  virtual void create_source(const CreateSourceRequest& request,
                             rpcz::reply<CreateSourceResponse> reply) override;
  virtual void delete_source(const DeleteSourceRequest& request,
                             rpcz::reply<DeleteSourceResponse> reply) override;
  virtual void generate_id(const GenerateIdRequest& request,
                           rpcz::reply<GenerateIdResponse> reply) override;
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

  inline void async_call_listeners(RequestType req_type, const std::string& request);

  inline int64_t generate_random_index();
  inline uuid_t generate_uuid();

  std::vector<std::string> ids_for_put_request(const PutRecordsRequest& request);

  RiakProxy& riak_proxy_;
  sql::ConnectionFactory& sql_factory_;
  boost::asio::io_service& ios_;

  boost::uuids::random_generator uuid_generator_;
  std::mutex uuid_generator_mutex_;
  std::function<int64_t()> uniform_distribution_;
  std::mutex uniform_distribution_mutex_;

  uint32_t max_generate_id_count_;
  const std::string sources_data_bucket_prefix_;

  std::vector<ListenerInfo> listeners_;
};

}}  // namespace reinferio::saltfish

#endif  // REINFERIO_SALTFISH_SERVICE_HPP
