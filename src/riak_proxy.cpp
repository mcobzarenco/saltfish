#include "riak_proxy.hpp"


namespace rio {
namespace saltfish {


RiakProxy::RiakProxy(const string& host, int port) {
    this->connection = riak::make_single_socket_transport(host, port, ios);
    store_ptr store = riak::make_client(connection, &random_sibling_resolution, ios);

    store->get_object("test", "test2", std::bind(&print_object_value, _1, _2, _3));

    ios.run();

}


void RiakProxy::get_object(const string& bucket, const string& k, riak::get_response_handler handler) {
    store->get_object(bucket, k, handler);
}

std::shared_ptr<riak::object> random_sibling_resolution(const riak::siblings&) {
    std::cout << "Siblings being resolved!" << std::endl;
    auto new_content = std::make_shared<riak::object>();
    new_content->set_value("<result of sibling resolution>");
    return new_content;
}


void print_object_value(const std::error_code& error,
				   std::shared_ptr<riak::object> object,
				   riak::value_updater& update_value) {
    if (not error) {
    	if (!! object)
    	    std::cout << "Fetch succeeded! Value is: " << object->value() << std::endl;
    	else
    	    std::cout << "Fetch succeeded! No value found." << std::endl;
    } else {
	std::cout << "Could not receive the object from Riak due to a network or server error." << std::endl;
    }

    // string new_value = "dsa";
    // if (not error) {
    // 	if (!! object)
    // 	    std::cout << "Fetch succeeded! Value is: " << object->value() << std::endl;
    // 	else
    // 	    std::cout << "Fetch succeeded! No value found." << std::endl;

    // 	std::cout << "Putting new value: " << new_value << std::endl;
    // 	auto new_key = std::make_shared<riak::object>();
    // 	new_key->set_value(new_value);
    // 	riak::put_response_handler put_handler = std::bind(&handle_put_result, _1);
    // 	update_value(new_key, put_handler);
    // } else {
    // 	std::cout << "Could not receive the object from Riak due to a network or server error." << std::endl;
}



} // namespace rio
} // namespace saltfish
