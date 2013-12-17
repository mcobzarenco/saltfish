#define BOOST_BIND_NO_PLACEHOLDERS

#include "service.hpp"

#include "riak_proxy.hpp"
#include "service_utils.hpp"

#include <riak/client.hxx>
#include <glog/logging.h>
#include <boost/uuid/uuid_io.hpp>

#include <thread>
#include <sstream>
#include <set>
#include <chrono>
#include <random>


namespace reinferio {
namespace saltfish {

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
          {PutRecordsResponse::WRONG_NUMBER_OF_IDS,
                "The number of records ids does not match the number of records"},
          {PutRecordsResponse::INVALID_RECORD, "Invalid record"},
          {PutRecordsResponse::NETWORK_ERROR, NETWORK_ERROR_MESSAGE}
        });
  if(message != nullptr)
    send_reply(reply, status, message);
  else
    send_reply(reply, status,  error_messages[status]);
}

inline std::function<int64_t()> init_uniform_distribution() noexcept {
  unsigned int seed = chrono::system_clock::now().time_since_epoch().count();
  default_random_engine generator(seed);
  uniform_int_distribution<int64_t> distribution(
      numeric_limits<int64_t>::min(), numeric_limits<int64_t>::max());
  return bind(distribution, generator);
}
} //  anonymous namespace


SaltfishServiceImpl::SaltfishServiceImpl(
    RiakProxy& riak_proxy,
    sql::ConnectionPool& sql_pool,
    uint32_t max_generate_id_count,
    const string& sources_data_bucket_prefix,
    const string& sources_metadata_bucket)
    : riak_proxy_(riak_proxy),
      sql_pool_(sql_pool),
      uuid_generator_(),
      uniform_distribution_(init_uniform_distribution()),
      max_generate_id_count_(max_generate_id_count),
      sources_metadata_bucket_(sources_metadata_bucket),
      sources_data_bucket_prefix_(sources_data_bucket_prefix) {
}

void SaltfishServiceImpl::register_listener(
    RequestType req_type, Listener&& listener) {
  listens_to_.emplace_back(req_type);
  listeners_.emplace_back(listener);
  CHECK_EQ(listens_to_.size(), listeners_.size())
      << "Invalid mapping between request types and handlers";
}

void SaltfishServiceImpl::call_listeners(
    RequestType request_type, const string& request) {
  CHECK_EQ(listens_to_.size(), listeners_.size())
      << "Invalid mapping between request types and handlers";
  auto listener = listeners_.cbegin();
  for (auto listen = listens_to_.cbegin();
       listen != listens_to_.cend();
       ++listen, ++listener) {
    if(*listen == request_type || *listen == RequestType::ALL) {
      (*listener)(*listen, request);
    }
  }
}


/***********                      create_source                     ***********/

void SaltfishServiceImpl::create_source_handler(
    const string& source_id,
    const CreateSourceRequest& request,
    rpcz::reply<CreateSourceResponse> reply,
    const std::error_code& error,
    std::shared_ptr<riak::object> object,
    riak::value_updater& update_value) {
  if (!error) {
    if (object) {  // There's already a source with the same id
      // TODO(cristicbz): Extract to functions that check equality & equivalence
      // of protobufs.
      if (request.source().SerializeAsString() == object->value()) {
        // Trying to create a source that already exists with identical schema
        // Nothing to do - such that the call is idempotent
        CreateSourceResponse response;
        response.set_status(CreateSourceResponse::OK);
        response.set_source_id(source_id);
        reply.send(response);
      } else {
        LOG(INFO) << "A source with the same id, but different schema "
                  << "already exists (source_id=" << source_id << ")";
        reply_with_status(CreateSourceResponse::SOURCE_ID_ALREADY_EXISTS, reply);
      }
    } else {
      auto new_value = std::make_shared<riak::object>();
      request.source().SerializeToString(new_value->mutable_value());
      riak::put_response_handler handler =
        [source_id, reply](const std::error_code& error) mutable {
        if (!error) {
          VLOG(0) << "Successfully created source (id=" << source_id << ")";
          CreateSourceResponse response;
          response.set_status(CreateSourceResponse::OK);
          response.set_source_id(source_id);
          reply.send(response);
        } else {
          LOG(ERROR) << "Could not receive the object from Riak due to a network "
          << "or server error: " << error;
          reply_with_status(CreateSourceResponse::NETWORK_ERROR, reply);
        }
      };

      // std::bind(&create_source_put_handler, source_id, reply, _1);
      update_value(new_value, handler);
    }
  } else {
    LOG(ERROR) << "Could not receive the object from Riak due to a network or "
               << "server error: " << error;
    reply_with_status(CreateSourceResponse::NETWORK_ERROR, reply);
  }
}

void SaltfishServiceImpl::create_source(
    const CreateSourceRequest& request,
    rpcz::reply<CreateSourceResponse> reply) {
  static constexpr char CREATE_SOURCE_TEMPLATE[] =
      "INSERT INTO sources (source_id, user_id, `schema`, name) "
      "VALUES (%0q:source_id, %1q:user_id, %2q:schema, %3q:name)";

  const auto& source = request.source();
  if (schema_has_duplicates(source.schema())) {
    reply_with_status(CreateSourceResponse::DUPLICATE_FEATURE_NAME, reply);
    return;
  }

  string source_id;
  if (source.source_id().size() == boost::uuids::uuid::static_size()) {
    source_id = source.source_id();
  } else if (source.source_id().empty()) {
    LOG(INFO) << "Request source_id not set, generating one" ;
    boost::uuids::uuid uuid = generate_uuid();
    source_id.assign(uuid.begin(), uuid.end());
  } else {
    LOG(INFO) << "create_source(): invalid source_id";
    reply_with_status(CreateSourceResponse::INVALID_SOURCE_ID, reply);
    return;
  }
  call_listeners(RequestType::CREATE_SOURCE, request.SerializeAsString());

  auto handler = bind(&SaltfishServiceImpl::create_source_handler,
                      this, source_id, request, reply, _1,  _2,  _3);
  LOG(INFO) << "creating source (id=" << source_id
            << ", schema='" << source.schema().ShortDebugString() << "')";
  riak_proxy_.get_object(sources_metadata_bucket_, source_id, handler);

  try {
    mysqlpp::ScopedConnection conn(sql_pool_, true);
    mysqlpp::Query query(conn->query(CREATE_SOURCE_TEMPLATE));
    query.parse();
    query.execute(source_id,
                  source.user_id(),
                  source.schema().SerializeAsString(),
                  source.name());
  } catch (const mysqlpp::BadQuery& e) {
    LOG(ERROR) << "Query error: " << e.what();
    //reply_with_status(CreateSourceResponse::UNKNOWN_ERROR, reply);
  } catch (const mysqlpp::Exception& e) {
    LOG(ERROR) << "MariaDB connection error: " << e.what();
    reply_with_status(CreateSourceResponse::NETWORK_ERROR, reply);
  }
}

/***********                      delete_source                     ***********/

void SaltfishServiceImpl::delete_source(
    const DeleteSourceRequest& request,
    rpcz::reply<DeleteSourceResponse> reply) {
  // TODO: Make sure the actual data is deleted by some job later
  const string& source_id = request.source_id();
  VLOG(0) << "delete_source(source_id=" << source_id << ")";

  riak::put_response_handler handler =
      [source_id, reply](const std::error_code& error) mutable {
    if(!error) {
      VLOG(0) << "Source deleted successfully (id=" << source_id << ")";
      reply_with_status(DeleteSourceResponse::OK, reply);
    } else {
      LOG(INFO) << "Deletion failed: " << error;
      reply_with_status(DeleteSourceResponse::NETWORK_ERROR, reply);
    }
    return;
  };

  riak_proxy_.delete_object(sources_metadata_bucket_,
                            request.source_id(),
                            handler);
}

/***********                       generate_id                      ***********/

uuid_t SaltfishServiceImpl::generate_uuid() {
  std::lock_guard<std::mutex> generate_uuid_lock(uuid_generator_mutex_);
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

namespace {

void put_records_put_handler(shared_ptr<PutRecordsReplier> replier,
                             const std::error_code& error) {
  if (!error) {
    replier->reply(PutRecordsResponse::OK, {});
  } else {
    replier->reply(PutRecordsResponse::NETWORK_ERROR,
                   "Could not connect to the storage backend");
  }
  return;
}

void put_records_get_handler(const source::Record& record,
                             const int64_t index_value,
                             shared_ptr<PutRecordsReplier> replier,
                             const std::error_code& error,
                             std::shared_ptr<riak::object> object,
                             riak::value_updater& update_value) {
  if (!error) {
    auto new_record = std::make_shared<riak::object>();
    record.SerializeToString(new_record->mutable_value());

    riak::rpair *index = new_record->add_indexes();
    index->set_key("randomindex_int");
    index->set_value(to_string(index_value).c_str());

    riak::put_response_handler handler =
        std::bind(&put_records_put_handler, replier, std::placeholders::_1);
    update_value(new_record, handler);
  } else {
    replier->reply(PutRecordsResponse::NETWORK_ERROR,
                   "Could not connect to the storage backend");
  }
  return;
}

}  // namespace


void SaltfishServiceImpl::put_records_check_handler(
    const PutRecordsRequest& request,
    rpcz::reply<PutRecordsResponse> reply,
    const std::error_code& error,
    std::shared_ptr<riak::object> object,
    riak::value_updater& update_value) {
  if (!error) {
    if (object) {
      source::Source source;
      if (!source.ParseFromString(object->value())) {
        LOG(ERROR) << "Could not parse source metadata for request: "
                   << request.DebugString();
        reply_with_status(PutRecordsResponse::INVALID_SCHEMA, reply,
                          "The source does not a valid schema");
        return;
      }
      const source::Schema& schema = source.schema();
      const auto& records = request.records();
      pair<bool, string> result =
              put_records_check_schema(schema, records.begin(), records.end());
      if (!result.first) {
        VLOG(0) << "Invalid record in put_records request: " << result.second;
        reply_with_status(PutRecordsResponse::INVALID_RECORD, reply,
                          result.second.c_str());
        return;
      }

      uint32_t n_record_ids = request.record_ids_size();
      uint32_t n_records = request.records_size();
      vector<string> record_ids;
      if (n_record_ids == 0) {
        for (uint32_t i = 0; i < n_records; ++i)
          record_ids.emplace_back(
              move(boost::uuids::to_string(generate_uuid())));
      } else {
        CHECK_EQ(n_record_ids, n_records)
            << "Expected the same number of records and record_ids";
        record_ids.assign(request.record_ids().begin(),
                          request.record_ids().end());
      }

      auto replier = make_shared<PutRecordsReplier>(record_ids, reply);
      for (uint32_t i = 0; i < n_records; ++i) {
        const string& record_id = record_ids[i];
        const source::Record& record = request.records(i);
        auto handler = bind(&put_records_get_handler, record,
                            generate_random_index(),  replier, _1,  _2,  _3);
        ostringstream bucket;
        bucket << sources_data_bucket_prefix_ << request.source_id() << "/";
        VLOG(0) << "Queueing put_record @ (b=" << bucket.str()
                << " k=" << record_id << ")";
        riak_proxy_.get_object(bucket.str(), record_id, handler);
      }
    } else {
      VLOG(0) << "Received put_records request for non-existent source; id="
              << request.source_id();
      string msg = "Source does not exist (id=";
      msg = msg + request.source_id() + ")";
      reply_with_status(PutRecordsResponse::INVALID_SOURCE_ID,
                        reply, msg.c_str());
    }
  } else {
    LOG(ERROR) << "Could not receive the object from Riak due to a network or "
               << "server error.";
    reply_with_status(PutRecordsResponse::NETWORK_ERROR, reply);
  }
}

/*
void put_handler(const PutRecordsRequest& request,
		 rpcz::reply<saltfish::PutRecordsResponse> reply,
		 const std::error_code& error,
		 std::shared_ptr<riak::object> object,
		 riak::value_updater& update_value) {
  if(!error) {
    if (object)
      LOG(INFO) << "Fetch succeeded! Value is: " << object->value();
    else
      LOG(INFO) << "Fetch succeeded! No value found.";

    PutRecordsResponse response;
    response.set_status(PutRecordsResponse::OK);
    reply.send(response);

    rio::source::FeatureSchema ft;
    ft.set_name("feat");
    ft.set_feature_type(rio::source::FeatureSchema::CATEGORICAL);

    auto row = request.source_row();
    // std::cout<< "byte size = " << row.ByteSize() << std::endl;

    // std::cout << "Putting new value: " << s << std::endl;
    auto new_key = std::make_shared<riak::object>();
    row.SerializeToString(new_key->mutable_value());
    riak::rpair *index = new_key->add_indexes();
    index->set_key("someindex_bin");
    index->set_value("1");
    riak::put_response_handler put_handler = std::bind(&handle_put_result, response, std::placeholders::_1);
    update_value(new_key, put_handler);
  } else {
    LOG(ERROR) << "Could not receive the object from Riak due to a network or server error.";
    PutRecordsResponse response;
    response.set_status(PutRecordsResponse::ERROR);
    response.set_msg("Could not connect to the storage backend");
    reply.send(response);
  }
  return;
}
*/


void SaltfishServiceImpl::put_records(
    const PutRecordsRequest& request,
    rpcz::reply<PutRecordsResponse> reply) {
  uint32_t n_record_ids = request.record_ids_size();
  uint32_t n_records = request.records_size();
  if (request.source_id().empty()) {
    VLOG(0) << "Got put_records request with empty";
    reply_with_status(PutRecordsResponse::INVALID_SOURCE_ID, reply,
                      "The source id is not set in the request.");
    return;
  }
  if (n_records == 0) {
    VLOG(0) << "Empty put_records request";
    reply_with_status(PutRecordsResponse::NO_RECORDS_IN_REQUEST, reply);
    return;
  }
  if (n_record_ids > 0 && n_record_ids != n_records) {
    VLOG(0) << "Got invalid put_records request with n_records_ids != n_records";
    reply_with_status(PutRecordsResponse::WRONG_NUMBER_OF_IDS, reply);
    return;
  }

  auto handler = bind(&SaltfishServiceImpl::put_records_check_handler,
                      this, request, reply, _1, _2, _3);
  riak_proxy_.get_object(sources_metadata_bucket_, request.source_id(), handler);
}

}}  // namespace reinferio::saltfish
