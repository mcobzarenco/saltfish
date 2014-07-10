#define BOOST_BIND_NO_PLACEHOLDERS
#include "service.hpp"
#include "service_utils.hpp"
#include "sql_errors.hpp"

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

using store::sql_error_category;
using store::SqlErr;

namespace { //  Some utility functions

constexpr uint32_t SOURCE_ID_WIDTH{24};

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
          {CreateSourceResponse::DUPLICATE_SOURCE_NAME,
                sql_error_category().message(SqlErr::DUPLICATE_SOURCE_NAME)},
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
          {PutRecordsResponse::INVALID_SOURCE_ID,
                sql_error_category().message(SqlErr::INVALID_SOURCE_ID)},
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

inline void reply_with_status(
    GetSourcesResponse::Status status,
    rpcz::reply<GetSourcesResponse>& reply,
    const char* message = nullptr) {
  static const vector<const char *> error_messages =
      make_error_messages<GetSourcesResponse>({
          {GetSourcesResponse::UNKNOWN_ERROR, UNKNOWN_ERROR_MESSAGE},
          {GetSourcesResponse::OK, ""},
          {GetSourcesResponse::INVALID_SOURCE_ID,
                sql_error_category().message(SqlErr::INVALID_SOURCE_ID)},
          {GetSourcesResponse::INVALID_USER_ID,
                sql_error_category().message(SqlErr::INVALID_USER_ID)},
          {GetSourcesResponse::INVALID_USERNAME,
                sql_error_category().message(SqlErr::INVALID_USERNAME)},
          {GetSourcesResponse::INVALID_REQUEST,
                "Exactly one field should be set in the request."},
          {GetSourcesResponse::NETWORK_ERROR, NETWORK_ERROR_MESSAGE}
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
    const string& sources_data_bucket_prefix,
    const string& schemas_bucket)
    :  riak_client_{riak_client},
       sql_store_{sql_store},
       ios_{ios},
       max_generate_id_count_{max_generate_id_count},
       sources_data_bucket_prefix_{sources_data_bucket_prefix},
       schemas_bucket_{schemas_bucket} {
}

void SaltfishServiceImpl::async_call_listeners(
    RequestType req_type, const string& request) {
  for (auto& listener : listeners_) {
    if(listener.listens_to == req_type ||
       listener.listens_to == RequestType::ALL) {
      auto handler = bind(listener.handler, req_type, request);
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
            << b64encode(source_id)
            << ", schema='" << source.schema().ShortDebugString() << "')";

  if (!new_source_id) {
    core::Schema remote_schema;
    error_condition sql_response = sql_store_.fetch_schema(remote_schema, source_id);

    if (sql_response == SqlErr::OK) {
      if (remote_schema.SerializeAsString() ==
          source.schema().SerializeAsString()) {
        // Trying to create a source that already exists with identical schema
        // Nothing to do - send OK such that the call is idempotent
        CreateSourceResponse response;
        response.set_status(CreateSourceResponse::OK);
        response.set_source_id(source_id);
        reply.send(response);
        return;
      } else {
        LOG(WARNING) << "A source with the same id, but different schema "
                     << "already exists (source_id="
                     << b64encode(source_id) << ")";
        reply_with_status(
            CreateSourceResponse::SOURCE_ID_ALREADY_EXISTS, reply);
        return;
      }
    } else if (sql_response != SqlErr::INVALID_SOURCE_ID) {
      // It is OK if the source_id does not exist
      reply_with_status(CreateSourceResponse::NETWORK_ERROR, reply);
      return;
    }
  }
  auto sql_response = sql_store_.create_source(
      source_id, source.user_id(), source.schema().SerializeAsString(),
      source.name(), source.private_(), source.frozen());

  if (sql_response == SqlErr::OK) {
    // Store a copy of the schema (which is immutable anyway) in Riak
    riak::object object(schemas_bucket_, source_id);
    source.schema().SerializeToString(&object.value());
    riak_client_.store(
        object, [reply, source_id, this] (const error_code error) mutable {
          CreateSourceResponse response;
          auto status = error ?
              CreateSourceResponse::NETWORK_ERROR : CreateSourceResponse::OK;
          response.set_source_id(source_id);
          response.set_status(status);
          reply.send(response);
        });
  } else {
    CreateSourceResponse response;
    switch (static_cast<SqlErr>(sql_response.value())) {
      case SqlErr::INVALID_USER_ID:
        response.set_status(CreateSourceResponse::INVALID_USER_ID);
        break;
      case SqlErr::DUPLICATE_SOURCE_NAME:
        response.set_status(CreateSourceResponse::DUPLICATE_SOURCE_NAME);
        break;
      default:
        response.set_status(CreateSourceResponse::NETWORK_ERROR);
    }
    reply.send(response);
  }
}

/***********                      delete_source                     ***********/

// TODO: Make sure the actual data is deleted by some job later
void SaltfishServiceImpl::delete_source(
    const DeleteSourceRequest& request,
    rpcz::reply<DeleteSourceResponse> reply) {
  const string& source_id = request.source_id();
  if (source_id.size() != SOURCE_ID_WIDTH) {
    LOG(INFO) << "delete_source() with invalid source_id";
    reply_with_status(DeleteSourceResponse::INVALID_SOURCE_ID, reply);
    return;
  }
  VLOG(0) << "delete_source(source_id=" << b64encode(source_id) << ")";
  int rows_updated;
  error_condition sql_response =
      sql_store_.delete_source(rows_updated, source_id);
  if (sql_response == SqlErr::OK) {
    CHECK(rows_updated == 0 || rows_updated == 1)
        << "source_id is a primary key, a max of 1 row can be affected";
    if (rows_updated == 0) {
      reply_with_status(DeleteSourceResponse::OK, reply);
    } else {
      async_call_listeners(
          RequestType::DELETE_SOURCE, request.SerializeAsString());
      DeleteSourceResponse response;
      response.set_status(DeleteSourceResponse::OK);
      response.set_updated(true);
      reply.send(response);
      return;
    }
  } else {
    reply_with_status(DeleteSourceResponse::NETWORK_ERROR, reply);
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

/***********                      get_sources                      ***********/

void SaltfishServiceImpl::get_sources(
    const GetSourcesRequest& request,
    rpcz::reply<GetSourcesResponse> reply) {
  if (request.has_source_id() + request.has_user_id() +
      request.has_username() != 1) {
    reply_with_status(GetSourcesResponse::INVALID_REQUEST, reply);
    return;
  }
  if (request.has_source_id()) {
    GetSourcesResponse response;
    auto& source_info = *response.add_sources_info();
    error_condition sql_response =
      sql_store_.get_source_by_id(source_info, request.source_id());
    if (sql_response == SqlErr::OK) {
      response.set_status(GetSourcesResponse::OK);
      reply.send(response);
      return;
    } else if(sql_response == SqlErr::INVALID_SOURCE_ID) {
      reply_with_status(GetSourcesResponse::INVALID_SOURCE_ID, reply);
      return;
    }
  } else {
    vector<SourceInfo> sources_info;
    error_condition sql_response;
    if (request.has_user_id()) {
      sql_response = sql_store_.get_sources_by_user(
          sources_info, request.user_id());
    } else {
      sql_response = sql_store_.get_sources_by_username(
          sources_info, request.username());
    }
    if (sql_response == SqlErr::OK) {
      GetSourcesResponse response;
      for (auto& source_info : sources_info) {
        *response.add_sources_info() = source_info;
      }
      response.set_status(GetSourcesResponse::OK);
      reply.send(response);
      return;
    }
  }
  reply_with_status(GetSourcesResponse::NETWORK_ERROR, reply);
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
  core::Schema schema;
  error_condition sql_response = sql_store_.fetch_schema(schema, source_id);
  if (sql_response == SqlErr::INVALID_SOURCE_ID) {
    VLOG(0) << "Received put_records request for non-existent source; id="
            << b64encode(source_id);
    stringstream msg;
    msg << "Trying to put records into non-existent source (id="
        << b64encode(source_id) << ")";
    reply_with_status(PutRecordsResponse::INVALID_SOURCE_ID, reply,
                      msg.str().c_str());
    return;
  } else if (sql_response != SqlErr::OK) {
    reply_with_status(PutRecordsResponse::NETWORK_ERROR, reply);
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
  bucket_ss << sources_data_bucket_prefix_ << b64encode(source_id);
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

    VLOG(0) << "Queueing put_record @ (b=" << bucket << " k="
            << *reinterpret_cast<const int64_t*>(record_id.c_str()) << ")";
    // riak_client_.fetch(bucket.str(), record_id, handler);
    fetch_closures.emplace_back([this, bucket, record_id, handler] () {
        this->riak_client_.fetch(
            bucket, record_id,
            function<void(riak::object, const error_code)>{handler});
      });
  }
  for_each(fetch_closures.begin(), fetch_closures.end(),
           [](function<void()>& closure) { closure(); });

  // TODO: This is incorrect, needs to call listeners only if successful
  async_call_listeners(RequestType::PUT_RECORDS, request.SerializeAsString());
}

}}  // namespace reinferio::saltfish
