#include "service.hpp"

#include <sstream>


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
      LOG(INFO) << "create_source | Found existing source with id=" << source_id
                << "; schema=" << schema_to_str(current_schema);

      LOG(INFO) << "create_source | schemas are the same: "
                << (request.schema().SerializeAsString() == object->value());

      if (request.schema().SerializeAsString() == object->value()) {
        // Trying to create a source that already exists with identical schema
        // Nothing to do - such that the call is idempotent
        CreateSourceResponse response;
        response.set_status(CreateSourceResponse::OK);
        response.set_source_id(source_id);
        reply.send(response);
      } else {
        CreateSourceResponse response;
        response.set_status(CreateSourceResponse::ERROR);
        response.set_msg("A source with the same id, but different schema already exists");
        reply.send(response);
      }
      return;
    }
    else
      LOG(INFO) << "Fetch succeeded! No value found." ;
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

void handle_put_result(rpcz::reply<saltfish::PushRowsResponse>& response,
		       const std::error_code& error) {
  if (not error) {
    std::cout << "Successfully put value" << std::endl;
    PushRowsResponse resp;
    resp.set_status(PushRowsResponse::OK);
    response.send(resp);
  } else {
    std::cerr << "Could not put value." << std::endl;
  }
  return;
}

void confirm_put(const PushRowsRequest& request,
		 rpcz::reply<saltfish::PushRowsResponse>& reply,
		 const std::error_code& error,
		 std::shared_ptr<riak::object> object,
		 riak::value_updater& update_value) {
  if(!error) {
    if (object)
      LOG(INFO) << "Fetch succeeded! Value is: " << object->value();
    else
      LOG(INFO) << "Fetch succeeded! No value found." ;

    PushRowsResponse response;
    response.set_status(PushRowsResponse::OK);
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
    PushRowsResponse response;
    response.set_status(PushRowsResponse::ERROR);
    response.set_msg("Could not connect to the storage backend");
    reply.send(response);
  }
  return;
}

SourceManagerService::SourceManagerService(RiakProxy* riak_proxy_)
    :riak_proxy(riak_proxy_) {
}

void SourceManagerService::create_source(const CreateSourceRequest& request,
                                         rpcz::reply<saltfish::CreateSourceResponse> reply) {
  // uuid_t uuid = uuid_generator();
  auto source_id = request.source_id();
  auto handler = bind(&create_source_get_handler, source_id, request, reply,
                      ph::_1,  ph::_2,  ph::_3);
  LOG(INFO) << "create_source(source_id=" << source_id
	    << ", schema=" << schema_to_str(request.schema()) << ")";
  riak_proxy->get_object(SOURCES_META_BUCKET, source_id, handler);
}

void SourceManagerService::delete_source(const DeleteSourceRequest& request,
                                         rpcz::reply<saltfish::DeleteSourceResponse> reply) {
}

void SourceManagerService::generate_id(const GenerateIdRequest& request,
                                       rpcz::reply<saltfish::GenerateIdResponse> reply) {
  LOG(INFO) << "Generating " << request.count() << " ids";

  GenerateIdResponse response;
  if(request.count() < MAX_GENERATE_ID_COUNT) {
    response.set_status(GenerateIdResponse::OK);
    for(uint32_t i = 0; i < request.count(); ++i) {
      string *id = response.add_ids();
      *id = boost::uuids::to_string(uuid_generator());
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

void SourceManagerService::push_rows(const PushRowsRequest& request,
                                     rpcz::reply<saltfish::PushRowsResponse> reply) {
  uuid_t uuid = uuid_generator();
  auto handler = bind(&confirm_put, request, reply, ph::_1,  ph::_2,  ph::_3);
  LOG(INFO) << "Adding datapoint to " << request.source_id() << "/" << uuid;
  riak_proxy->get_object(request.source_id(), boost::uuids::to_string(uuid), handler);
}


}  // namespace reinferio
}  // namespace saltfish
