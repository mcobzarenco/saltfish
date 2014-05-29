#define BOOST_BIND_NO_PLACEHOLDERS
#include "service.hpp"
#include "service_utils.hpp"

#include <riakpp/connection.hpp>
#include <glog/logging.h>
#include <boost/optional.hpp>

#include <algorithm>
#include <chrono>
#include <functional>
#include <random>
#include <set>
#include <sstream>
#include <thread>
#include <vector>


namespace reinferio { namespace saltfish {

using namespace std;
using namespace std::placeholders;

namespace { //  Some utility functions

constexpr uint32_t SOURCE_ID_WIDTH{16};

//  Error messages:
constexpr char UNKNOWN_ERROR_MESSAGE[]{
  "Unknown error status: must likely using protobufs with mismatched versions."};
constexpr char NETWORK_ERROR_MESSAGE[]{
  "Internal error: could not connect to the storage backend."};

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
                "A source with the same id, but different schema already exists."},
          {CreateSourceResponse::INVALID_SOURCE_ID,
                "The source id provided is invalid."},
          {CreateSourceResponse::INVALID_USER_ID,
                "The user id provided is invalid."},
          {CreateSourceResponse::INVALID_FEATURE_TYPE,
                "The schema contains invalid feature types."},
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

} //  anonymous namespace


SaltfishServiceImpl::SaltfishServiceImpl(
    riak::client& riak_client,
    store::MetadataSqlStoreTasklet& sql_store,
    boost::asio::io_service& ios,
    uint32_t max_generate_id_count,
    const string& sources_data_bucket_prefix)
    :  riak_client_{riak_client},
       sql_store_{sql_store},
       ios_{ios},
       max_generate_id_count_{max_generate_id_count},
       sources_data_bucket_prefix_{sources_data_bucket_prefix} {
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
  } else if (schema_has_invalid_features(source.schema())) {
    reply_with_status(CreateSourceResponse::INVALID_FEATURE_TYPE, reply);
    LOG(INFO) << "INVALID_FEATURE_TYPE";
    return;
  }
  string source_id;
  bool new_source_id{false};
  if (source.source_id().empty()) {
    LOG(INFO) << "create_source() request source_id not set, generating one" ;
    source_id = gen_random_string(SOURCE_ID_WIDTH);
    new_source_id = true;
  } else if (source.source_id().size() == SOURCE_ID_WIDTH) {
    source_id = source.source_id();
  } else {
    LOG(INFO) << "create_source() invalid source_id";
    reply_with_status(CreateSourceResponse::INVALID_SOURCE_ID, reply);
    return;
  }
  LOG(INFO) << "create_source() inserting source (id="
            << string_to_hex(source_id)
            << ", schema='" << source.schema().ShortDebugString() << "')";

  if (!new_source_id) {
    auto remote_schema = sql_store_.fetch_schema(source_id);
    if(!remote_schema) {
      reply_with_status(CreateSourceResponse::NETWORK_ERROR, reply);
      return;
    }
    if (remote_schema->size() > 0) {
      if (remote_schema->front() ==
          source.schema().SerializeAsString()) {
        // Trying to create a source that already exists with identical schema
        // Nothing to do - send OK such that the call is idempotent
        CreateSourceResponse response;
        response.set_status(CreateSourceResponse::OK);
        response.set_source_id(source_id);
        reply.send(response);
        return;
      } else {
        LOG(INFO) << "A source with the same id, but different schema "
                  << "already exists (source_id="
                  << string_to_hex(source_id) << ")";
        reply_with_status(
            CreateSourceResponse::SOURCE_ID_ALREADY_EXISTS, reply);
        return;
      }
    }
  }
  auto resp = sql_store_.create_source(source_id,
                                       source.user_id(),
                                       source.schema().SerializeAsString(),
                                       source.name());
  CreateSourceResponse response;
  if (resp) {
    async_call_listeners(RequestType::CREATE_SOURCE, request.SerializeAsString());
    response.set_status(CreateSourceResponse::OK);
    response.set_source_id(source_id);
  } else {
    response.set_status(CreateSourceResponse::NETWORK_ERROR);
  }
  reply.send(response);
}

/***********                      delete_source                     ***********/

// TODO: Make sure the actual data is deleted by some job later
void SaltfishServiceImpl::delete_source(
    const DeleteSourceRequest& request,
    rpcz::reply<DeleteSourceResponse> reply) {
  const string& source_id = request.source_id();
  if (source_id.size() == boost::uuids::uuid::static_size()) {
    VLOG(0) << "delete_source(source_id=" << string_to_hex(source_id) << ")";
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

void SaltfishServiceImpl::generate_id(
    const GenerateIdRequest& request,
    rpcz::reply<GenerateIdResponse> reply) {
  VLOG(0) << "generate_id(count=" << request.count() << ")";
  GenerateIdResponse response;
  if(request.count() < max_generate_id_count_) {
    response.set_status(GenerateIdResponse::OK);
    for(uint32_t i = 0; i < request.count(); ++i) {
      response.add_ids(gen_random_string(SOURCE_ID_WIDTH));
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

/***********                      list_sources                      ***********/

void SaltfishServiceImpl::list_sources(
    const ListSourcesRequest& request,
    rpcz::reply<ListSourcesResponse> reply) {
  using maybe_sources = boost::optional<vector<core::Source>>;
  const int user_id{request.user_id()};
  maybe_sources sources{sql_store_.list_sources(user_id)};

  ListSourcesResponse response;
  if (sources) {
    response.set_status(ListSourcesResponse::OK);
    for_each(sources->begin(), sources->end(),
             [&](const core::Source& src) {
               *(response.add_sources()) = src;
             });
  } else {
    response.set_status(ListSourcesResponse::NETWORK_ERROR);
  }
  reply.send(response);
}

/***********                       put_records                      ***********/

vector<string> SaltfishServiceImpl::ids_for_put_request(
    const PutRecordsRequest& request) {
  vector<string> record_ids;
  for (auto i = 0; i < request.records_size(); ++i) {
    auto tag_record = request.records(i);
    if (tag_record.record_id().empty()) {
      const int64_t index = gen_random_int64();
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

void put_records_get_handler(
    riak::client& riak_client, const core::Record record,
    const int64_t index_value, shared_ptr<ReplySync> replier,
    rpcz::reply<PutRecordsResponse> reply,
    riak::object object, const error_code error) {
  if (!error) {
    // auto* index = new_record->add_indexes();
    // index->set_key("randomindex_int");
    // index->set_value(to_string(index_value).c_str());
    record.SerializeToString(&object.value());
    auto handler = bind(&put_records_put_handler, replier, reply, _1);
    riak_client.store(object, function<void(const error_code)>{handler});
  } else {
    LOG(WARNING) << "Trying fetch() from Riak bucket=" << object.bucket()
                 << " key="
                 << *reinterpret_cast<const int64_t*>(object.key().data())
                 <<" got error_code="  << error;
    replier->error([reply] () mutable {
        LOG(INFO) << "REPLYING WITH ERROR";
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
  if (source_id.size() != SOURCE_ID_WIDTH) {
    VLOG(0) << "Got put_records request with an invalid source id";
    reply_with_status(PutRecordsResponse::INVALID_SOURCE_ID, reply,
                      "The source id provided is invalid.");
    return;
  } else  if (n_records == 0) {
    VLOG(0) << "Empty put_records request";
    reply_with_status(PutRecordsResponse::NO_RECORDS_IN_REQUEST, reply);
    return;
  }
  auto schema_vec = sql_store_.fetch_schema(source_id);
  if (!schema_vec) {
    reply_with_status(PutRecordsResponse::UNKNOWN_ERROR, reply);
    return;
  }
  if (schema_vec->size() == 0) {
    VLOG(0) << "Received put_records request for non-existent source; id="
            << string_to_hex(source_id);
    stringstream msg;
    msg << "Trying to put records into non-existent source (id="
        << string_to_hex(source_id) << ")";
    reply_with_status(PutRecordsResponse::INVALID_SOURCE_ID, reply,
                      msg.str().c_str());
    return;
  }
  core::Schema schema;
  if (!schema.ParseFromString(schema_vec->front())) {
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
  stringstream bucket_ss;
  bucket_ss << sources_data_bucket_prefix_ << string_to_hex(source_id);
  string bucket{bucket_ss.str()};

  auto replier = make_shared<ReplySync>(request.records_size(), reply_success);
  vector<function<void()>> fetch_closures;
  fetch_closures.reserve(request.records_size());
  for (auto i = 0; i < request.records_size(); ++i) {
    const auto& record_id = record_ids[i];
    const auto& record = request.records(i).record();
    auto handler = bind(&put_records_get_handler, ref(riak_client_), record,
                        *reinterpret_cast<const int64_t*>(record_id.c_str()),
                        replier, reply, _1,  _2);

    VLOG(0) << "Queueing put_record @ (b=" << bucket
            << " k=" << *reinterpret_cast<const int64_t*>(record_id.c_str()) << ")";
    // riak_client_.fetch(bucket.str(), record_id, handler);
    fetch_closures.emplace_back([this, bucket, record_id, handler] () {
        this->riak_client_.fetch(
            bucket, record_id,
            function<void(riak::object, const error_code)>{handler});
      });
  }
  for_each(fetch_closures.begin(), fetch_closures.end(),
           [](function<void()>& closure) { closure(); });

  // async_call_listeners(RequestType::CREATE_SOURCE, request.SerializeAsString());
}

}}  // namespace reinferio::saltfish
