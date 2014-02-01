#define BOOST_BIND_NO_PLACEHOLDERS
#include "service.hpp"
#include "service_utils.hpp"

#include <riakpp/connection.hpp>
#include <glog/logging.h>
#include <boost/uuid/uuid_io.hpp>
#include <boost/optional.hpp>

#include <cppconn/exception.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>
#include <mysql_driver.h>
#include <mysql_connection.h>

#include <chrono>
#include <functional>
#include <thread>
#include <sstream>
#include <set>
#include <random>


namespace reinferio { namespace saltfish {

using namespace std;
using namespace std::placeholders;

//  Error messages:
constexpr char UNKNOWN_ERROR_MESSAGE[] =
    "Unknown error status: must likely using protobufs with mismatched versions.";
constexpr char NETWORK_ERROR_MESSAGE[] =
    "Internal error: could not connect to the storage backend.";

namespace { //  Some utility functions

template<typename Response>
inline vector<const char*> make_error_messages(
    initializer_list<pair<typename Response::Status, const char*>> status_msgs) {
  int max = -1;
  for (auto status_msg : status_msgs)
    if (max < status_msg.first)
      max = status_msg.first;
  CHECK_GE(max, 1)
      << "Every response message type should have at least 2 status codes";

  vector<const char*> messages(max + 1);
  for (auto status_msg : status_msgs)
    messages[status_msg.first] = status_msg.second;
  return messages;
}

template<typename Response>
inline void send_reply(rpcz::reply<Response>& reply,
                       typename Response::Status status,
                       const char* message) {
  Response response;
  response.set_status(status);
  response.set_msg(message);
  reply.send(response);
}

inline void reply_with_status(
    CreateSourceResponse::Status status,
    rpcz::reply<CreateSourceResponse>& reply,
    const char* message = nullptr) {
  static const vector<const char *> error_messages =
      make_error_messages<CreateSourceResponse>({
          {CreateSourceResponse::UNKNOWN_ERROR, UNKNOWN_ERROR_MESSAGE},
          {CreateSourceResponse::OK, ""},
          {CreateSourceResponse::DUPLICATE_FEATURE_NAME,
                "The provided schema contains duplicate feature names."},
          {CreateSourceResponse::SOURCE_ID_ALREADY_EXISTS,
                "A source with the same id, but different schema already exists"},
          {CreateSourceResponse::INVALID_SOURCE_ID,
                "The source id provided is invalid"},
          {CreateSourceResponse::NETWORK_ERROR, NETWORK_ERROR_MESSAGE}
        });

  if(message != nullptr)
    send_reply(reply, status, message);
  else
    send_reply(reply, status,  error_messages[status]);
}

inline void reply_with_status(
    DeleteSourceResponse::Status status,
    rpcz::reply<DeleteSourceResponse>& reply,
    const char* message = nullptr) {
  static const vector<const char *> error_messages =
      make_error_messages<DeleteSourceResponse>({
          {DeleteSourceResponse::UNKNOWN_ERROR, UNKNOWN_ERROR_MESSAGE},
          {DeleteSourceResponse::OK, ""},
          {DeleteSourceResponse::INVALID_SOURCE_ID,
                "The source id provided is invalid"},
          {DeleteSourceResponse::NETWORK_ERROR, NETWORK_ERROR_MESSAGE}
        });
  if(message != nullptr)
    send_reply(reply, status, message);
  else
    send_reply(reply, status,  error_messages[status]);
}

inline void reply_with_status(
    PutRecordsResponse::Status status,
    rpcz::reply<PutRecordsResponse>& reply,
    const char* message = nullptr) {
  // Not all of the messages below are actually used
  // as the argument message is used
  static const vector<const char *> error_messages =
      make_error_messages<PutRecordsResponse>({
          {PutRecordsResponse::UNKNOWN_ERROR, UNKNOWN_ERROR_MESSAGE},
          {PutRecordsResponse::OK, ""},
          {PutRecordsResponse::INVALID_SCHEMA, "Invalid schema"},
          {PutRecordsResponse::INVALID_SOURCE_ID, "Invalid source id."},
          {PutRecordsResponse::NO_RECORDS_IN_REQUEST,
                "No records in the request."},
          {PutRecordsResponse::INVALID_RECORD, "Invalid record"},
          {PutRecordsResponse::NETWORK_ERROR, NETWORK_ERROR_MESSAGE}
        });
  if(message != nullptr)
    send_reply(reply, status, message);
  else
    send_reply(reply, status,  error_messages[status]);
}

inline std::function<int64_t()> init_uniform_distribution() noexcept {
  long seed{chrono::system_clock::now().time_since_epoch().count()};
  default_random_engine generator{seed};
  uniform_int_distribution<int64_t> distribution{
    numeric_limits<int64_t>::min(), numeric_limits<int64_t>::max()};
  return bind(distribution, generator);
}

} //  anonymous namespace


SaltfishServiceImpl::SaltfishServiceImpl(
    riak::client& riak_client,
    store::SourceMetadataSqlStoreTasklet& sql_store,
    boost::asio::io_service& ios,
    uint32_t max_generate_id_count,
    const string& sources_data_bucket_prefix)
    : riak_client_(riak_client),
      sql_store_(sql_store),
      ios_(ios),
      uuid_generator_(),
      uniform_distribution_(init_uniform_distribution()),
      max_generate_id_count_(max_generate_id_count),
      sources_data_bucket_prefix_(sources_data_bucket_prefix) {
}

void SaltfishServiceImpl::async_call_listeners(
    RequestType req_type, const string& request) {
  for (auto& listener : listeners_) {
    if(listener.listens_to == req_type ||
       listener.listens_to == RequestType::ALL) {
      auto handler = bind(listener.handler, listener.listens_to, request);
      listener.strand.post(handler);
    }
  }
}

/***********                      create_source                     ***********/

void SaltfishServiceImpl::create_source(
    const CreateSourceRequest& request,
    rpcz::reply<CreateSourceResponse> reply) {
  const auto& source = request.source();
  if (schema_has_duplicates(source.schema())) {
    reply_with_status(CreateSourceResponse::DUPLICATE_FEATURE_NAME, reply);
    return;
  }
  string source_id;
  bool new_source_id{false};
  if (source.source_id().empty()) {
    LOG(INFO) << "create_source() request source_id not set, generating one" ;
    boost::uuids::uuid uuid = generate_uuid();
    source_id.assign(uuid.begin(), uuid.end());
    new_source_id = true;
  } else if (is_valid_uuid_bytes(source.source_id())) {
    source_id = source.source_id();
  } else {
    LOG(INFO) << "create_source() invalid source_id";
    reply_with_status(CreateSourceResponse::INVALID_SOURCE_ID, reply);
    return;
  }
  LOG(INFO) << "create_source() inserting source (id="
            << uuid_bytes_to_hex(source_id)
            << ", schema='" << source.schema().ShortDebugString() << "')";
  try {
    if (!new_source_id) {
      auto remote_schema = sql_store_.fetch_schema(source_id);
      if (remote_schema) {
        if (*remote_schema == request.source().schema().SerializeAsString()) {
          // Trying to create a source that already exists with identical schema
          // Nothing to do - such that the call is idempotent
          CreateSourceResponse response;
          response.set_status(CreateSourceResponse::OK);
          response.set_source_id(source_id);
          reply.send(response);
          return;
        } else {
          LOG(INFO) << "A source with the same id, but different schema "
                    << "already exists (source_id="
                    << uuid_bytes_to_hex(source_id) << ")";
          reply_with_status(CreateSourceResponse::SOURCE_ID_ALREADY_EXISTS, reply);
          return;
        }
      }
    }
    sql_store_.create_source(source_id, source.user_id(),
                             source.schema().SerializeAsString(), source.name());

    async_call_listeners(RequestType::CREATE_SOURCE, request.SerializeAsString());
    CreateSourceResponse response;
    response.set_status(CreateSourceResponse::OK);
    response.set_source_id(source_id);
    reply.send(response);
  } catch (const ::sql::SQLException& e) {
    LOG(ERROR) << "create_source() query error: " << e.what();
    reply_with_status(CreateSourceResponse::UNKNOWN_ERROR, reply);
  }
}

/***********                      delete_source                     ***********/

// TODO: Make sure the actual data is deleted by some job later
void SaltfishServiceImpl::delete_source(
    const DeleteSourceRequest& request,
    rpcz::reply<DeleteSourceResponse> reply) {
  const string& source_id = request.source_id();
  if (source_id.size() == boost::uuids::uuid::static_size()) {
    VLOG(0) << "delete_source(source_id="
            << boost::uuids::to_string(from_string(source_id)) << ")";
    try {
      auto rows_updated = sql_store_.delete_source(source_id);
      CHECK(rows_updated == 0 || rows_updated == 1)
          << "source_id is a primary key, a max of 1 row can be affected";
      if (rows_updated == 0) {
        reply_with_status(DeleteSourceResponse::OK, reply);
      } else {
        async_call_listeners(RequestType::DELETE_SOURCE,
                             request.SerializeAsString());
        DeleteSourceResponse response;
        response.set_status(DeleteSourceResponse::OK);
        response.set_updated(true);
        reply.send(response);
      }
    } catch (const ::sql::SQLException& e) {
      LOG(ERROR) << "delete_source() query error: " << e.what();
      reply_with_status(DeleteSourceResponse::UNKNOWN_ERROR, reply);
    }
  } else {
    LOG(INFO) << "delete_source() with invalid source_id";
    reply_with_status(DeleteSourceResponse::INVALID_SOURCE_ID, reply);
  }
}

/***********                       generate_id                      ***********/

uuid_t SaltfishServiceImpl::generate_uuid() {
  lock_guard<mutex> generate_uuid_lock(uuid_generator_mutex_);
  return uuid_generator_();
}

void SaltfishServiceImpl::generate_id(
    const GenerateIdRequest& request,
    rpcz::reply<GenerateIdResponse> reply) {
  VLOG(0) << "generate_id(count=" << request.count() << ")";
  GenerateIdResponse response;
  if(request.count() < max_generate_id_count_) {
    response.set_status(GenerateIdResponse::OK);
    for(uint32_t i = 0; i < request.count(); ++i) {
      response.add_ids(boost::uuids::to_string(generate_uuid()));
    }
  } else {
    response.set_status(GenerateIdResponse::COUNT_TOO_LARGE);
    ostringstream msg;
    msg << "Cannot generate more than " << max_generate_id_count_
        << " in one call (" << request.count() << " requested)";
    response.set_msg(msg.str());
  }
  reply.send(response);
}

/***********                       put_records                      ***********/

int64_t SaltfishServiceImpl::generate_random_index() {
  std::lock_guard<std::mutex>
      uniform_distribution_lock(uniform_distribution_mutex_);
  return uniform_distribution_();
}

vector<string> SaltfishServiceImpl::ids_for_put_request(
    const PutRecordsRequest& request) {
  vector<string> record_ids;
  for (auto i = 0; i < request.records_size(); ++i) {
    auto tag_record = request.records(i);
    if (tag_record.record_id().empty()) {
      const int64_t index = generate_random_index();
      record_ids.emplace_back(
          reinterpret_cast<const char *>(&index), sizeof(int64_t));
    } else if (tag_record.record_id().size() == sizeof(int64_t)) {
      record_ids.emplace_back(tag_record.record_id());
    }
  }
  return move(record_ids);
}

namespace {

void put_records_put_handler(shared_ptr<ReplySync> replier,
                             rpcz::reply<PutRecordsResponse> reply,
                             const error_code error) {
  if (!error) {
    replier->ok();
  } else {
    replier->error([&reply] () {
        reply_with_status(PutRecordsResponse::NETWORK_ERROR, reply,
                          "Could not connect to the storage backend");
      });
  }
}

void put_records_get_handler(riak::client& riak_client,
                             const source::Record record,
                             const int64_t index_value,
                             shared_ptr<ReplySync> replier,
                             rpcz::reply<PutRecordsResponse> reply,
                             riak::object object,
                             const error_code error) {
  if (!error) {
    record.SerializeToString(&object.value());
    // auto* index = new_record->add_indexes();
    // index->set_key("randomindex_int");
    // index->set_value(to_string(index_value).c_str());
    auto handler = bind(&put_records_put_handler, replier, reply, _1);
    riak_client.store(object, function<void(const error_code)>{handler});
  } else {
    replier->error([&reply] () {
        reply_with_status(PutRecordsResponse::NETWORK_ERROR, reply,
                          "Could not connect to the storage backend");
      });
  }
}

}  // namespace

void SaltfishServiceImpl::put_records(
    const PutRecordsRequest& request,
    rpcz::reply<PutRecordsResponse> reply) {
  const auto& source_id = request.source_id();
  const uint32_t n_records = request.records_size();
  // LOG(INFO) << request.DebugString();
  if (!is_valid_uuid_bytes(source_id)) {
    VLOG(0) << "Got put_records request with an invalid source id";
    reply_with_status(PutRecordsResponse::INVALID_SOURCE_ID, reply,
                      "The source id provided is invalid.");
    return;
  } else  if (n_records == 0) {
    VLOG(0) << "Empty put_records request";
    reply_with_status(PutRecordsResponse::NO_RECORDS_IN_REQUEST, reply);
    return;
  }
  try {
    auto schema_str = sql_store_.fetch_schema(source_id);
    if (!schema_str) {
      VLOG(0) << "Received put_records request for non-existent source; id="
              << uuid_bytes_to_hex(source_id);
      stringstream msg;
      msg << "Source does not exist (id="
          << uuid_bytes_to_hex(source_id) << ")";
      reply_with_status(PutRecordsResponse::INVALID_SOURCE_ID, reply);
      return;
    }
    source::Schema schema;
    if (!schema.ParseFromString(*schema_str)) {
      LOG(ERROR) << "Could not parse source schema for request: "
                 << request.DebugString();
      reply_with_status(PutRecordsResponse::INVALID_SCHEMA, reply,
                        "The source does not a valid schema");
      return;
    }
    for(uint32_t ix = 0; ix < n_records; ++ix) {
      if (auto err = check_record(schema, request.records(ix).record())) {
        stringstream msg;
        msg << "At position " << ix << ": " << err.what();
        VLOG(0) << "Invalid record in put_records request: " << msg.str();
        reply_with_status(PutRecordsResponse::INVALID_RECORD, reply,
                          msg.str().c_str());
        return;
      }
    }
    auto record_ids = ids_for_put_request(request);
    CHECK_EQ(request.records_size(), record_ids.size());
    auto reply_success = [reply, record_ids] () mutable {
      PutRecordsResponse response;
      response.set_status(PutRecordsResponse::OK);
      for (const auto& rid : record_ids) response.add_record_ids(rid);
      reply.send(response);
    };
    auto replier = make_shared<ReplySync>(
        request.records_size(), reply_success);
    for (auto i = 0; i < request.records_size(); ++i) {
      const auto& record_id = record_ids[i];
      const auto& record = request.records(i).record();
      auto handler = bind(&put_records_get_handler, ref(riak_client_), record,
                          *reinterpret_cast<const int64_t*>(record_id.c_str()),
                          replier, reply, _1,  _2);
      ostringstream bucket;
      bucket << sources_data_bucket_prefix_
             << uuid_bytes_to_hex(source_id);
      VLOG(0) << "Queueing put_record @ (b=" << bucket.str()
              << " k=" << *reinterpret_cast<const int64_t*>(record_id.c_str()) << ")";
      // riak_client_.fetch(bucket.str(), record_id, handler);
      riak_client_.fetch(bucket.str(), record_id,
                         function<void(riak::object, const error_code)>{handler});
    }
    // async_call_listeners(RequestType::CREATE_SOURCE, request.SerializeAsString());
  } catch (const ::sql::SQLException& e) {
    LOG(ERROR) << "create_source() query error: " << e.what();
    reply_with_status(PutRecordsResponse::UNKNOWN_ERROR, reply);
  }
}

}}  // namespace reinferio::saltfish
