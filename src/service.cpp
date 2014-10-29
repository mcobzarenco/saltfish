#define BOOST_BIND_NO_PLACEHOLDERS
#include "service.hpp"
#include "service_utils.hpp"
#include "sql_errors.hpp"

#include <glog/logging.h>
#include <boost/optional.hpp>

#include <algorithm>
#include <chrono>
#include <functional>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace reinferio { namespace saltfish {

using namespace std;
using namespace std::placeholders;

using store::sql_error_category;
using store::SqlErr;

namespace { //  Some utility functions

constexpr uint32_t DATASET_ID_WIDTH{24};

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
    CreateDatasetResponse::Status status,
    rpcz::reply<CreateDatasetResponse>& reply,
    const char* message = nullptr) {
  static const vector<const char *> error_messages =
      make_error_messages<CreateDatasetResponse>({
          {CreateDatasetResponse::UNKNOWN_ERROR, UNKNOWN_ERROR_MESSAGE},
          {CreateDatasetResponse::OK, ""},
          {CreateDatasetResponse::DUPLICATE_FEATURE_NAME,
                "The provided schema contains duplicate feature names."},
          {CreateDatasetResponse::DUPLICATE_DATASET_NAME,
                sql_error_category().message(SqlErr::DUPLICATE_DATASET_NAME)},
          {CreateDatasetResponse::DATASET_ID_ALREADY_EXISTS,
                "A dataset with the same id, but different schema already exists."},
          {CreateDatasetResponse::INVALID_DATASET_ID,
                "The dataset id provided is invalid."},
          {CreateDatasetResponse::INVALID_USER_ID,
                "The user id provided is invalid."},
          {CreateDatasetResponse::INVALID_FEATURE_TYPE,
                "The schema contains invalid feature types."},
          {CreateDatasetResponse::NETWORK_ERROR, NETWORK_ERROR_MESSAGE}
        });

  if (message != nullptr)
    send_reply(reply, status, message);
  else
    send_reply(reply, status,  error_messages[status]);
}

inline void reply_with_status(
    DeleteDatasetResponse::Status status,
    rpcz::reply<DeleteDatasetResponse>& reply,
    const char* message = nullptr) {
  static const vector<const char *> error_messages =
      make_error_messages<DeleteDatasetResponse>({
          {DeleteDatasetResponse::UNKNOWN_ERROR, UNKNOWN_ERROR_MESSAGE},
          {DeleteDatasetResponse::OK, ""},
          {DeleteDatasetResponse::INVALID_DATASET_ID,
                "The dataset id provided is invalid"},
          {DeleteDatasetResponse::NETWORK_ERROR, NETWORK_ERROR_MESSAGE}
        });
  if (message != nullptr)
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
          {PutRecordsResponse::INVALID_DATASET_ID,
                sql_error_category().message(SqlErr::INVALID_DATASET_ID)},
          {PutRecordsResponse::NO_RECORDS_IN_REQUEST,
                "No records in the request."},
          {PutRecordsResponse::INVALID_RECORD, "Invalid record"},
          {PutRecordsResponse::NETWORK_ERROR, NETWORK_ERROR_MESSAGE}
        });
  if (message != nullptr)
    send_reply(reply, status, message);
  else
    send_reply(reply, status,  error_messages[status]);
}

inline void reply_with_status(
    GetDatasetsResponse::Status status,
    rpcz::reply<GetDatasetsResponse>& reply,
    const char* message = nullptr) {
  static const vector<const char *> error_messages =
      make_error_messages<GetDatasetsResponse>({
          {GetDatasetsResponse::UNKNOWN_ERROR, UNKNOWN_ERROR_MESSAGE},
          {GetDatasetsResponse::OK, ""},
          {GetDatasetsResponse::INVALID_DATASET_ID,
                sql_error_category().message(SqlErr::INVALID_DATASET_ID)},
          {GetDatasetsResponse::INVALID_USER_ID,
                sql_error_category().message(SqlErr::INVALID_USER_ID)},
          {GetDatasetsResponse::INVALID_USERNAME,
                sql_error_category().message(SqlErr::INVALID_USERNAME)},
          {GetDatasetsResponse::INVALID_REQUEST,
                "Exactly one field should be set in the request."},
          {GetDatasetsResponse::NETWORK_ERROR, NETWORK_ERROR_MESSAGE}
        });
  if (message != nullptr)
    send_reply(reply, status, message);
  else
    send_reply(reply, status,  error_messages[status]);
}


} //  anonymous namespace


DatasetStoreImpl::DatasetStoreImpl(
    riak::client& riak_client,
    store::MetadataSqlStoreTasklet& sql_store,
    boost::asio::io_service& ios,
    uint32_t max_generate_id_count,
    const string& records_bucket_prefix,
    const string& schemas_bucket,
    const uint64_t max_random_index)
    :  riak_client_{riak_client},
       sql_store_{sql_store},
       ios_{ios},
       max_generate_id_count_{max_generate_id_count},
       records_bucket_prefix_{records_bucket_prefix},
       schemas_bucket_{schemas_bucket},
       max_random_index_{max_random_index} {
}

void DatasetStoreImpl::async_call_listeners(
    RequestType req_type, const string& request) {
  for (auto& listener : listeners_) {
    if (listener.listens_to == req_type ||
       listener.listens_to == RequestType::ALL) {
      auto handler = bind(listener.handler, req_type, request);
      listener.strand.post(handler);
    }
  }
}

/***********                      create_dataset                     ***********/


void DatasetStoreImpl::create_dataset(
    const CreateDatasetRequest& request,
    rpcz::reply<CreateDatasetResponse> reply) {
  const auto& dataset = request.dataset();
  if (schema_has_duplicates(dataset.schema())) {
    reply_with_status(CreateDatasetResponse::DUPLICATE_FEATURE_NAME, reply);
    return;
  } else if (schema_has_invalid_features(dataset.schema())) {
    reply_with_status(CreateDatasetResponse::INVALID_FEATURE_TYPE, reply);
    return;
  }
  string dataset_id;
  bool new_dataset_id{false};
  if (dataset.id().empty()) {
    LOG(INFO) << "create_dataset() request dataset_id not set, generating one";
    dataset_id = gen_random_string(DATASET_ID_WIDTH);
    new_dataset_id = true;
  } else if (dataset.id().size() == DATASET_ID_WIDTH) {
    dataset_id = dataset.id();
  } else {
    LOG(INFO) << "create_dataset() invalid dataset_id";
    reply_with_status(CreateDatasetResponse::INVALID_DATASET_ID, reply);
    return;
  }
  LOG(INFO) << "create_dataset() inserting dataset (id="
            << b64encode(dataset_id)
            << ", schema='" << dataset.schema().ShortDebugString() << "')";

  if (!new_dataset_id) {
    core::Schema remote_schema;
    error_condition sql_response = sql_store_.fetch_schema(remote_schema, dataset_id);

    if (sql_response == SqlErr::OK) {
      if (remote_schema.SerializeAsString() ==
          dataset.schema().SerializeAsString()) {
        // Trying to create a dataset that already exists with identical schema
        // Nothing to do - send OK such that the call is idempotent
        CreateDatasetResponse response;
        response.set_status(CreateDatasetResponse::OK);
        response.set_dataset_id(dataset_id);
        reply.send(response);
        return;
      } else {
        LOG(WARNING) << "A dataset with the same id, but different schema "
                     << "already exists (dataset_id="
                     << b64encode(dataset_id) << ")";
        reply_with_status(
            CreateDatasetResponse::DATASET_ID_ALREADY_EXISTS, reply);
        return;
      }
    } else if (sql_response != SqlErr::INVALID_DATASET_ID) {
      // It is OK if the dataset_id does not exist
      reply_with_status(CreateDatasetResponse::NETWORK_ERROR, reply);
      return;
    }
  }
  auto sql_response = sql_store_.create_dataset(
      dataset_id, dataset.user_id(), dataset.schema().SerializeAsString(),
      dataset.name(), dataset.private_(), dataset.frozen());

  if (sql_response == SqlErr::OK) {
    // Store a copy of the schema (which is immutable anyway) in Riak
    riak::object object(schemas_bucket_, dataset_id);
    dataset.schema().SerializeToString(&object.value());
    riak_client_.async_store(
        object, [reply, dataset_id, this] (const error_code error) mutable {
          CreateDatasetResponse response;
          auto status = error ?
              CreateDatasetResponse::NETWORK_ERROR : CreateDatasetResponse::OK;
          response.set_dataset_id(dataset_id);
          response.set_status(status);
          reply.send(response);
        });
  } else {
    CreateDatasetResponse response;
    switch (static_cast<SqlErr>(sql_response.value())) {
      case SqlErr::INVALID_USER_ID:
        response.set_status(CreateDatasetResponse::INVALID_USER_ID);
        break;
      case SqlErr::DUPLICATE_DATASET_NAME:
        response.set_status(CreateDatasetResponse::DUPLICATE_DATASET_NAME);
        break;
      default:
        response.set_status(CreateDatasetResponse::NETWORK_ERROR);
    }
    reply.send(response);
  }
}

/***********                      delete_dataset                     ***********/

// TODO: Make sure the actual data is deleted by some job later
void DatasetStoreImpl::delete_dataset(
    const DeleteDatasetRequest& request,
    rpcz::reply<DeleteDatasetResponse> reply) {
  const string& dataset_id = request.dataset_id();
  if (dataset_id.size() != DATASET_ID_WIDTH) {
    LOG(INFO) << "delete_dataset() with invalid dataset_id";
    reply_with_status(DeleteDatasetResponse::INVALID_DATASET_ID, reply);
    return;
  }
  VLOG(0) << "delete_dataset(dataset_id=" << b64encode(dataset_id) << ")";
  int rows_updated;
  error_condition sql_response =
      sql_store_.delete_dataset(rows_updated, dataset_id);
  if (sql_response == SqlErr::OK) {
    CHECK(rows_updated == 0 || rows_updated == 1)
        << "dataset_id is a primary key, a max of 1 row can be affected";
    if (rows_updated == 0) {
      reply_with_status(DeleteDatasetResponse::OK, reply);
    } else {
      async_call_listeners(
          RequestType::DELETE_DATASET, request.SerializeAsString());
      DeleteDatasetResponse response;
      response.set_status(DeleteDatasetResponse::OK);
      response.set_updated(true);
      reply.send(response);
      return;
    }
  } else {
    reply_with_status(DeleteDatasetResponse::NETWORK_ERROR, reply);
  }
}

/***********                       generate_id                      ***********/

void DatasetStoreImpl::generate_id(
    const GenerateIdRequest& request,
    rpcz::reply<GenerateIdResponse> reply) {
  VLOG(0) << "generate_id(count=" << request.count() << ")";
  GenerateIdResponse response;
  if (request.count() < max_generate_id_count_) {
    response.set_status(GenerateIdResponse::OK);
    for (uint32_t i = 0; i < request.count(); ++i) {
      response.add_ids(gen_random_string(DATASET_ID_WIDTH));
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

/***********                      get_datasets                      ***********/

void DatasetStoreImpl::get_datasets(
    const GetDatasetsRequest& request,
    rpcz::reply<GetDatasetsResponse> reply) {
  if (request.has_dataset_id() + request.has_user_id() +
      request.has_username() != 1) {
    reply_with_status(GetDatasetsResponse::INVALID_REQUEST, reply);
    return;
  }
  if (request.has_dataset_id()) {
    GetDatasetsResponse response;
    auto& dataset_detail = *response.add_datasets();
    error_condition sql_response =
      sql_store_.get_dataset_by_id(dataset_detail, request.dataset_id());
    if (sql_response == SqlErr::OK) {
      response.set_status(GetDatasetsResponse::OK);
      reply.send(response);
      return;
    } else if (sql_response == SqlErr::INVALID_DATASET_ID) {
      reply_with_status(GetDatasetsResponse::INVALID_DATASET_ID, reply);
      return;
    }
  } else {
    vector<DatasetDetail> datasets_details;
    error_condition sql_response;
    if (request.has_user_id()) {
      sql_response = sql_store_.get_datasets_by_user(
          datasets_details, request.user_id());
    } else {
      sql_response = sql_store_.get_datasets_by_username(
          datasets_details, request.username());
    }
    if (sql_response == SqlErr::OK) {
      GetDatasetsResponse response;
      for (auto& dataset_info : datasets_details) {
        *response.add_datasets() = dataset_info;
      }
      response.set_status(GetDatasetsResponse::OK);
      reply.send(response);
      return;
    }
  }
  reply_with_status(GetDatasetsResponse::NETWORK_ERROR, reply);
}

/***********                       put_records                      ***********/

vector<string> DatasetStoreImpl::ids_for_put_request(
    const PutRecordsRequest& request) {
  vector<string> record_ids;
  for (auto i = 0; i < request.records_size(); ++i) {
    auto tag_record = request.records(i);
    if (tag_record.record_id().empty()) {
      const uint64_t index{gen_random_uint64()};
      record_ids.emplace_back(
          reinterpret_cast<const char *>(&index), sizeof(uint64_t));
    } else if (tag_record.record_id().size() == sizeof(int64_t)) {
      record_ids.emplace_back(tag_record.record_id());
    }
  }
  return move(record_ids);
}

namespace {

void add_riak_index(riak::object& object,
                    const string index_name, const string& value) {
  auto* index = object.raw_content().add_indexes();
    std::chrono::milliseconds(1);
  index->set_key(index_name);
  index->set_value(value);
}

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
    riak::client& riak_client, const core::Record& record,
    const uint64_t random_index, const uint64_t sequence_index,
    const string& source, shared_ptr<ReplySync> replier,
    rpcz::reply<PutRecordsResponse> reply,
    const error_code error, riak::object object) {
  if (!error) {
    int64_t timestamp{
      chrono::system_clock::now().time_since_epoch() / chrono::microseconds(1)};
    add_riak_index(object, "timestamp_int", to_string(timestamp));
    add_riak_index(object, "sequence_int", to_string(sequence_index));
    add_riak_index(object, "randomindex_int", to_string(random_index));
    if (!source.empty()) { add_riak_index(object, "source_bin", source); }

    record.SerializeToString(&object.value());
    auto handler = bind(&put_records_put_handler, replier, reply, _1);
    // TODO(cristicbz): Investigate why the cast is needed.
    riak_client.async_store(object, function<void(const error_code)>{handler});
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

void DatasetStoreImpl::put_records(
    const PutRecordsRequest& request,
    rpcz::reply<PutRecordsResponse> reply) {
  const auto& dataset_id = request.dataset_id();
  const uint32_t n_records = request.records_size();
  if (dataset_id.size() != DATASET_ID_WIDTH) {
    VLOG(0) << "Got put_records request with an invalid dataset id";
    reply_with_status(PutRecordsResponse::INVALID_DATASET_ID, reply,
                      "The dataset id provided is invalid.");
    return;
  } else  if (n_records == 0) {
    VLOG(0) << "Empty put_records request";
    reply_with_status(PutRecordsResponse::NO_RECORDS_IN_REQUEST, reply);
    return;
  }
  core::Schema schema;
  error_condition sql_response = sql_store_.fetch_schema(schema, dataset_id);
  if (sql_response == SqlErr::INVALID_DATASET_ID) {
    VLOG(0) << "Received put_records request for non-existent dataset; id="
            << b64encode(dataset_id);
    stringstream msg;
    msg << "Trying to put records into non-existent dataset (id="
        << b64encode(dataset_id) << ")";
    reply_with_status(PutRecordsResponse::INVALID_DATASET_ID, reply,
                      msg.str().c_str());
    return;
  } else if (sql_response != SqlErr::OK) {
    reply_with_status(PutRecordsResponse::NETWORK_ERROR, reply);
    return;
  }
  for (uint32_t ix = 0; ix < n_records; ++ix) {
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
  bucket_ss << records_bucket_prefix_ << b64encode(dataset_id);
  string bucket{bucket_ss.str()};

  auto replier = make_shared<ReplySync>(request.records_size(), reply_success);
  vector<function<void()>> fetch_closures;
  fetch_closures.reserve(request.records_size());
  for (auto i = 0; i < request.records_size(); ++i) {
    const auto& record_id = record_ids[i];
    const auto& record = request.records(i).record();
    const uint64_t random_index{gen_random_uint64() % max_random_index_};
    const uint64_t sequence{static_cast<uint64_t>(get_monotonous_ticks())};
    const string& source = request.source();

    auto handler = bind(&put_records_get_handler, ref(riak_client_), ref(record),
                        random_index, sequence, ref(source), replier, reply,
                        _1,  _2);
    VLOG(0) << "Queueing put_record @ (b=" << bucket << " k="
            << *reinterpret_cast<const int64_t*>(record_id.c_str()) << ")";
    // riak_client_.fetch(bucket.str(), record_id, handler);
    // TODO(cristicbz): Another case.
    fetch_closures.emplace_back([this, bucket, record_id, handler] () {
        this->riak_client_.async_fetch(
            bucket, record_id,
            function<void(const error_code, riak::object)>{handler});
      });
  }
  for_each(fetch_closures.begin(), fetch_closures.end(),
           [](function<void()>& closure) { closure(); });

  // TODO: This is incorrect, needs to call listeners only if successful
  async_call_listeners(RequestType::PUT_RECORDS, request.SerializeAsString());
}

}}  // namespace reinferio::saltfish
