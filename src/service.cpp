#include "service.hpp"


namespace reinferio {
namespace saltfish {

using namespace std;


void handle_put_result(rpcz::reply<saltfish::Response>& response,
		       const std::error_code& error) {
  if (not error) {
    std::cout << "Successfully put value" << std::endl;
    Response resp;
    resp.set_status(Response::OK);
    response.send(resp);
  } else {
    std::cerr << "Could not put value." << std::endl;
  }
  return;
}


void confirm_put(const Request& request,
		 rpcz::reply<saltfish::Response>& response,
		 const std::error_code& error,
		 std::shared_ptr<riak::object> object,
		 riak::value_updater& update_value) {
  if(!error) {
    if (object)
      cout << "Fetch succeeded! Value is: " << object->value() << endl;
    else
      cout << "Fetch succeeded! No value found." << endl;

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
    std::cout << "Could not receive the object from Riak due to a network or server error." << std::endl;
  }
  return;
}

SourceManagerService::SourceManagerService(RiakProxy* riak_proxy_)
    :riak_proxy(riak_proxy_) {
}

void SourceManagerService::push_rows(const Request& request,
				    rpcz::reply<saltfish::Response> response) {
  cout << &riak_proxy << std::endl;
  uuid_t uuid = uuid_generator();

  cout << "[" << std::this_thread::get_id()
       << "] Adding datapoint to " << request.source_id()
       << "/" << uuid << std::endl;

  auto handler = bind(&confirm_put, request, response,
                      placeholders::_1, placeholders::_2, placeholders::_3);
  riak_proxy->get_object(request.source_id(), boost::uuids::to_string(uuid), handler);
}


}  // namespace reinferio
}  // namespace saltfish
