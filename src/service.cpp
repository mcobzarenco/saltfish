#define BOOST_BIND_NO_PLACEHOLDERS

#include "service.hpp"

#include "riak_proxy.hpp"
#include "service_utils.hpp"

#include <riak/client.hxx>
#include <glog/logging.h>
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.

#include <thread>
#include <sstream>
#include <set>


namespace reinferio {
namespace saltfish {


const uint32_t MAX_GENERATE_ID_COUNT{1000};
const char SOURCES_METADATA_BUCKET[]{"/ml/sources/schemas/"};
const char SOURCES_DATA_BUCKET_ROOT[]{"/ml/sources/data/"};


using namespace std;
using namespace std::placeholders;


SourceManagerServiceImpl::SourceManagerServiceImpl(RiakProxy* riak_proxy)
    : riak_proxy_(riak_proxy), uuid_generator_() {
  CHECK_NOTNULL(riak_proxy);
}


/***********      SourceManagerService::create_source     ***********/

namespace {

void create_source_put_handler(const string& source_id,
                               rpcz::reply<CreateSourceResponse> reply,
                               const std::error_code& error) {
  CreateSourceResponse response;
  if (!error) {
    // TODO(mcobzarenco): Convert debug logs to VLOG(0).
    LOG(INFO) << "Successfully put value";
    response.set_status(CreateSourceResponse::OK);
    response.set_source_id(source_id);
    reply.send(response);
  } else {
    LOG(ERROR) << "Could not receive the object from Riak due to a network or "
               << "server error: " << error;
    response.set_status(CreateSourceResponse::NETWORK_ERROR);
    response.set_msg("Could not connect to the storage backend");
    reply.send(response);
  }
}

void create_source_get_handler(const string& source_id,
                               const CreateSourceRequest& request,
                               rpcz::reply<CreateSourceResponse> reply,
                               const std::error_code& error,
                               std::shared_ptr<riak::object> object,
                               riak::value_updater& update_value) {
  if (!error) {
    if (object) {  // There's already a source with the same id
      // TODO(cristicbz): Extract to functions that check equality & equivalence
      // of protobufs.
      if (request.schema().SerializeAsString() == object->value()) {
        // Trying to create a source that already exists with identical schema
        // Nothing to do - such that the call is idempotent
        CreateSourceResponse response;
        response.set_status(CreateSourceResponse::OK);
        response.set_source_id(source_id);
        reply.send(response);
      } else {
        LOG(WARNING) << "A source with the same id, but different schema already exists"
                     << " (source_id=" << source_id << ")";
        CreateSourceResponse response;
        response.set_status(CreateSourceResponse::SOURCE_ID_ALREADY_EXISTS);
        response.set_msg("A source with the same id, but different schema already exists");
        reply.send(response);
      }
    } else {
      auto new_value = std::make_shared<riak::object>();
      request.schema().SerializeToString(new_value->mutable_value());
      riak::put_response_handler handler =
          std::bind(&create_source_put_handler, source_id, reply, _1);
      update_value(new_value, handler);
    }
  } else {
    LOG(ERROR) << "Could not receive the object from Riak due to a network or "
               << "server error: " << error;
    CreateSourceResponse response;
    response.set_status(CreateSourceResponse::NETWORK_ERROR);
    response.set_msg("Could not connect to the storage backend");
    reply.send(response);
  }
}

}  // namespace

void SourceManagerServiceImpl::create_source(const CreateSourceRequest& request,
                                         rpcz::reply<CreateSourceResponse> reply) {
  if (schema_has_duplicates(request.schema())) {
    CreateSourceResponse response;
    response.set_status(CreateSourceResponse::DUPLICATE_FEATURE_NAME);
    // TODO(mcobzarenco): Error messages in constants.
    response.set_msg("The provided schema contains duplicate feature names.");
    reply.send(response);
    return;
  }

  string source_id;
  if (request.source_id().empty()) {
    LOG(INFO) << "Request source_id not set, generating one" ;
    source_id = boost::uuids::to_string(generate_uuid());
  } else {
    source_id = request.source_id();
  }
  auto handler = bind(&create_source_get_handler, source_id, request, reply,
                      _1,  _2,  _3);
  LOG(INFO) << "creating source (id=" << source_id
            << ", schema=" << schema_to_str(request.schema()) << ")";
  riak_proxy_->get_object(SOURCES_METADATA_BUCKET, source_id, handler);
}


/***********      SourceManagerServiceImpl::delete_source     ***********/

namespace {

void delete_source_handler(const string& source_id,
                           rpcz::reply<DeleteSourceResponse> reply,
                           const std::error_code& error) {
  DeleteSourceResponse response;
  if(!error) {
    LOG(INFO) << "Deletion successful";
    response.set_status(DeleteSourceResponse::OK);
  } else {
    LOG(INFO) << "Deletion failed: " << error;
    response.set_status(DeleteSourceResponse::NETWORK_ERROR);
  }
  reply.send(response);
  return;
}

}  // namespace

void SourceManagerServiceImpl::delete_source(const DeleteSourceRequest& request,
                                         rpcz::reply<DeleteSourceResponse> reply) {
  // TODO: Make sure the actual data is deleted by some job later
  LOG(INFO) << "delete_source(source_id=" << request.source_id() << ")";
  auto handler = bind(&delete_source_handler, request.source_id(), reply, _1);
  riak_proxy_->delete_object(SOURCES_METADATA_BUCKET, request.source_id(), handler);
}


/***********       SourceManagerService::generate_id       ***********/

uuid_t SourceManagerServiceImpl::generate_uuid() {
  lock_guard<mutex> generate_uuid_lock(uuid_generator_mutex_);
  return uuid_generator_();
}

void SourceManagerServiceImpl::generate_id(const GenerateIdRequest& request,
                                       rpcz::reply<GenerateIdResponse> reply) {
  LOG(INFO) << "generate_id(count=" << request.count() << ")";

  GenerateIdResponse response;
  if(request.count() < MAX_GENERATE_ID_COUNT) {
    response.set_status(GenerateIdResponse::OK);
    for(uint32_t i = 0; i < request.count(); ++i) {
      response.add_ids(boost::uuids::to_string(generate_uuid()));
    }
  } else {
    response.set_status(GenerateIdResponse::COUNT_TOO_LARGE);
    ostringstream msg;
    msg << "Cannot generate more than " << MAX_GENERATE_ID_COUNT
        << " in one call (" << request.count() << " requested)";
    response.set_msg(msg.str());
  }
  reply.send(response);
}


/***********       SourceManagerService::put_records       ***********/

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
                             shared_ptr<PutRecordsReplier> replier,
                             const std::error_code& error,
                             std::shared_ptr<riak::object> object,
                             riak::value_updater& update_value) {
  if (!error) {
    auto new_record = std::make_shared<riak::object>();
    record.SerializeToString(new_record->mutable_value());
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

void SourceManagerServiceImpl::put_records_check_handler(const PutRecordsRequest& request,
                                                     rpcz::reply<PutRecordsResponse> reply,
                                                     const std::error_code& error,
                                                     std::shared_ptr<riak::object> object,
                                                     riak::value_updater& update_value) {
  if (!error) {
    if (object) {
      source::Schema schema;
      schema.ParseFromString(object->value());
      pair<bool, string> result = put_records_check_schema(schema, request);
      if (!result.first) {
        LOG(INFO) << "Invalid record in put_records request: " << result.second;
        PutRecordsResponse response;
        response.set_status(PutRecordsResponse::INVALID_RECORD);
        response.set_msg(result.second);
        reply.send(response);
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
        auto handler = bind(&put_records_get_handler, record, replier,
                            _1,  _2,  _3);
        ostringstream bucket;
        bucket << SOURCES_DATA_BUCKET_ROOT << request.source_id() << "/";
        LOG(INFO) << "Queueing put_record @ (b=" << bucket.str()
                  << " k=" << record_id << ")";
        riak_proxy_->get_object(bucket.str(), record_id, handler);
      }
    } else {
      LOG(INFO) << "got put_records request for non-existent source";
      ostringstream msg;
      PutRecordsResponse response;
      response.set_status(PutRecordsResponse::INVALID_SOURCE_ID);
      msg << "Source does not exist (id=" << request.source_id() << ")";
      response.set_msg(msg.str());
      reply.send(response);
    }
  } else {
    LOG(ERROR) << "Could not receive the object from Riak due to a network or "
               << "server error.";
    PutRecordsResponse response;
    response.set_status(PutRecordsResponse::NETWORK_ERROR);
    response.set_msg("Could not connect to the storage backend");
    reply.send(response);
  }
}


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

    // rio::source::FeatureSchema ft;
    // ft.set_name("feat");
    // ft.set_feature_type(rio::source::FeatureSchema::CATEGORICAL);

    // auto row = request.source_row();
    // // std::cout<< "byte size = " << row.ByteSize() << std::endl;

    // // std::cout << "Putting new value: " << s << std::endl;
    // auto new_key = std::make_shared<riak::object>();
    // row.SerializeToString(new_key->mutable_value());
    // riak::rpair *index = new_key->add_indexes();
    // index->set_key("someindex_bin");
    // index->set_value("1");
    // riak::put_response_handler put_handler = std::bind(&handle_put_result, response, std::placeholders::_1);
    // update_value(new_key, put_handler);
  } else {
    LOG(ERROR) << "Could not receive the object from Riak due to a network or server error.";
    PutRecordsResponse response;
//    response.set_status(PutRecordsResponse::ERROR);
    response.set_msg("Could not connect to the storage backend");
    reply.send(response);
  }
  return;
}


void SourceManagerServiceImpl::put_records(const PutRecordsRequest& request,
                                       rpcz::reply<PutRecordsResponse> reply) {
  uint32_t n_record_ids = request.record_ids_size();
  uint32_t n_records = request.records_size();

  if (request.source_id().empty()) {
    LOG(INFO) << "source_id not set in a put_records request";
    PutRecordsResponse response;
    response.set_status(PutRecordsResponse::INVALID_SOURCE_ID);
    response.set_msg("source_id is empty");
    reply.send(response);
    return;
  }
  if (n_records == 0) {
    LOG(INFO) << "Got empty put_records request";
    PutRecordsResponse response;
    response.set_status(PutRecordsResponse::NO_RECORDS_IN_REQUEST);
    response.set_msg("no records in the request");
    reply.send(response);
    return;
  }
  if (n_record_ids > 0 && n_record_ids != n_records) {
    LOG(INFO) << "Got invalid put_records request with n_records_ids != n_records";
    PutRecordsResponse response;
    response.set_status(PutRecordsResponse::WRONG_NUMBER_OF_IDS);
    response.set_msg("n_records_ids != n_records");
    reply.send(response);
    return;
  }

  auto handler = bind(&SourceManagerServiceImpl::put_records_check_handler,
                      this, request, reply, _1, _2, _3);
  riak_proxy_->get_object(SOURCES_METADATA_BUCKET, request.source_id(), handler);
}


}  // namespace saltfish
}  // namespace reinferio
