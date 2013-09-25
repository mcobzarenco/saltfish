#include <riak/client.hxx>
#include <riak/transports/single_serial_socket.hxx>

#include <boost/asio.hpp>

#include <memory>
#include <iostream>


typedef std::shared_ptr<riak::client> store_ptr;

using namespace std::placeholders;


std::shared_ptr<riak::object> random_sibling_resolution (const ::riak::siblings&) {
    std::cout << "Siblings being resolved!" << std::endl;
    auto new_content = std::make_shared<riak::object>();
    new_content->set_value("<result of sibling resolution>");
    return new_content;
}


void handle_put_result(const std::error_code& error) {
    if (not error)
	std::cout << "Successfully put value" << std::endl;
    else
    	std::cerr << "Could not put value." << std::endl;
}


void print_object_value(const std::string& new_value,
			const std::error_code& error,
			std::shared_ptr<riak::object> object,
			riak::value_updater& update_value) {
    if (not error) {
        if (!! object)
            std::cout << "Fetch succeeded! Value is: " << object->value() << std::endl;
        else
            std::cout << "Fetch succeeded! No value found." << std::endl;

	std::cout << "Putting new value: " << new_value << std::endl;
	auto new_key = std::make_shared<riak::object>();
	new_key->set_value(new_value);
	riak::put_response_handler put_handler = std::bind(&handle_put_result, _1);
	update_value(new_key, put_handler);
    } else {
        std::cout << "Could not receive the object from Riak due to a network or server error." << std::endl;
    }
}


int main(int argc, char **argv) {

    boost::asio::io_service ios;
    auto connection = riak::make_single_socket_transport("localhost", 10017, ios);
    store_ptr store = riak::make_client(connection, &random_sibling_resolution, ios);

    store->get_object("test", "test2", std::bind(&print_object_value, "data2", _1, _2, _3));


    ios.run();
    return 0;
}
