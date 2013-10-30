#include "service.hpp"

#include <sstream>
#include <set>


namespace reinferio {
namespace saltfish {

using namespace std;
namespace ph = std::placeholders;

template<typename RequestT, typename ResponseT>
using GetHandler = std::function<void(RequestT, rpcz::reply<ResponseT>, const std::error_code&,
                                      std::shared_ptr<riak::object>, riak::value_updater&)>;


string schema_to_str(const source::Schema& schema) {
  auto ft_desc = source::Feature::FeatureType_descriptor();
  ostringstream ss;
  bool first = true;
  ss << "[";
  for (source::Feature f : schema.features()) {
    if(!first)
      ss << ", ";
    else
      first = false;
    ss << "(\"" << f.name() << "\":"
       << ft_desc->FindValueByNumber(f.feature_type())->name() << ")";
  }
  ss << "]";
  return ss.str();
}

bool schema_has_duplicates(const source::Schema& schema) {
  // TODO: Maybe use a more generic function that this..
  set<string> names;
  for (auto feature : schema.features()) {
    if (names.find(feature.name()) != names.end())
      return true;
    names.insert(feature.name());
  }
  return false;
}

void create_source_put_handler(const string& source_id,
                               rpcz::reply<CreateSourceResponse>& reply,
			       const std::error_code& error) {
  CreateSourceResponse response;
  if (!error) {
    LOG(INFO) << "Successfully put value";
    response.set_status(CreateSourceResponse::OK);
    response.set_source_id(source_id);
    reply.send(response);
  } else {
    LOG(ERROR) << "Could not receive the object from Riak due to a network or server error.";
    response.set_status(CreateSourceResponse::ERROR);
    response.set_msg("Could not connect to the storage backend");
    reply.send(response);
  }
  return;
}

void create_source_get_handler(const string& source_id,
			       const CreateSourceRequest& request,
			       rpcz::reply<CreateSourceResponse>& reply,
			       const std::error_code& error,
			       std::shared_ptr<riak::object> object,
                               riak::value_updater& update_value) {
  if (!error) {
    if (object) {  // There's already a source with the same id
      source::Schema current_schema;
      current_schema.ParseFromString(object->value());

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
        response.set_status(CreateSourceResponse::ERROR);
        response.set_msg("A source with the same id, but different schema already exists");
        reply.send(response);
      }
      return;
    }

    auto new_value = std::make_shared<riak::object>();
    request.schema().SerializeToString(new_value->mutable_value());
    riak::put_response_handler handler =
        std::bind(&create_source_put_handler, source_id, reply, std::placeholders::_1);
    update_value(new_value, handler);
  } else {
    LOG(ERROR) << "Could not receive the object from Riak due to a network or server error.";
    CreateSourceResponse response;
    response.set_status(CreateSourceResponse::ERROR);
    response.set_msg("Could not connect to the storage backend");
    reply.send(response);
  }
  return;
}

void delete_source_handler(const string& source_id,
                           rpcz::reply<DeleteSourceResponse>& reply,
                           const std::error_code& error) {
  DeleteSourceResponse response;
  if(!error) {
    LOG(INFO) << "Deletion successful";
    response.set_status(DeleteSourceResponse::OK);
  } else {
    LOG(INFO) << "Deletion failed: " << error;
    response.set_status(DeleteSourceResponse::ERROR);
  }
  reply.send(response);
  return;
}

void confirm_put(const PutRecordsRequest& request,
		 rpcz::reply<saltfish::PutRecordsResponse>& reply,
		 const std::error_code& error,
		 std::shared_ptr<riak::object> object,
		 riak::value_updater& update_value) {
  if(!error) {
    if (object)
      LOG(INFO) << "Fetch succeeded! Value is: " << object->value();
    else
      LOG(INFO) << "Fetch succeeded! No value found." ;

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
    response.set_status(PutRecordsResponse::ERROR);
    response.set_msg("Could not connect to the storage backend");
    reply.send(response);
  }
  return;
}

SourceManagerService::SourceManagerService(RiakProxy* riak_proxy)
    :riak_proxy_(riak_proxy), uuid_generator_() {
}

void SourceManagerService::create_source(const CreateSourceRequest& request,
                                         rpcz::reply<CreateSourceResponse> reply) {
  if (schema_has_duplicates(request.schema())) {
    CreateSourceResponse response;
    response.set_status(CreateSourceResponse::ERROR);
    response.set_msg("The provided schema contains duplicate feature names.");
    reply.send(response);
    return;
  }

  string source_id;
  if (request.source_id().empty()) {
    LOG(INFO) << "Request source_id not set, generating one" ;
    source_id = boost::uuids::to_string(uuid_generator_());
  } else {
    source_id = request.source_id();
  }
  auto handler = bind(&create_source_get_handler, source_id, request, reply,
                      ph::_1,  ph::_2,  ph::_3);
  LOG(INFO) << "creating source (id=" << source_id
	    << ", schema=" << schema_to_str(request.schema()) << ")";
  riak_proxy_->get_object(SOURCES_META_BUCKET, source_id, handler);
}


void SourceManagerService::delete_source(const DeleteSourceRequest& request,
                                         rpcz::reply<DeleteSourceResponse> reply) {
  // TODO: Make sure the actual data is deleted by some batch job later
  LOG(INFO) << "Deleting source_id=" << request.source_id();
  auto handler = bind(&delete_source_handler, request.source_id(), reply, ph::_1);
  riak_proxy_->delete_object(SOURCES_META_BUCKET, request.source_id(), handler);
}

void SourceManagerService::generate_id(const GenerateIdRequest& request,
                                       rpcz::reply<GenerateIdResponse> reply) {
  LOG(INFO) << "Generating " << request.count() << " ids";

  GenerateIdResponse response;
  if(request.count() < MAX_GENERATE_ID_COUNT) {
    response.set_status(GenerateIdResponse::OK);
    for(uint32_t i = 0; i < request.count(); ++i) {
      string *id = response.add_ids();
      *id = boost::uuids::to_string(uuid_generator_());
    }
  } else {
    response.set_status(GenerateIdResponse::ERROR);
    ostringstream ss;
    ss << "Cannot generate more than " << MAX_GENERATE_ID_COUNT
       << " in one call (" << request.count() << " requested)";
    response.set_msg(ss.str());
  }
  reply.send(response);
}

void SourceManagerService::put_records(const PutRecordsRequest& request,
                                       rpcz::reply<PutRecordsResponse> reply) {
  uuid_t uuid = uuid_generator_();
  auto handler = bind(&confirm_put, request, reply, ph::_1,  ph::_2,  ph::_3);
  LOG(INFO) << "Adding datapoint to " << request.source_id() << "/" << uuid;
  riak_proxy_->get_object(request.source_id(), boost::uuids::to_string(uuid), handler);
}


}  // namespace reinferio
}  // namespace saltfish
