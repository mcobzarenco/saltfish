#include "service.pb.h"
#include "service.rpcz.h"
#include "riak_proxy.hpp"

#include <rpcz/rpcz.hpp>

#include <iostream>


using namespace std;

namespace rio {
namespace saltfish {


class SourceManagerServer : public SourceManager {
    public:
    SourceManagerServer(std::shared_ptr<RiakProxy> riak_proxy);
    virtual void put_row(const source::SourceRow& request, rpcz::reply<saltfish::Response> response) {
	std::cout << "Received request: " << std::endl;
	Response resp;
	resp.set_status(Response::INVALID_SCHEMA);
	response.send(resp);
    }

    private:
    std::shared_ptr<RiakProxy> riak_proxy;

    // virtual void execute(const ::Query& request, rpcz::reply<Query> response) {
    // 	cout << "Got: \'" << request.query() << "\'" << endl;
    // 	Query query;
    // 	query.set_query(request.query());
    // 	response.send(query);
    // }

};

SourceManagerServer::SourceManagerServer(std::shared_ptr<RiakProxy> riak_proxy_):riak_proxy(riak_proxy_) {
}

}  // namespace rio
}  // namespace saltfish


int main() {
    rpcz::application application;
    rpcz::server server(application);
    auto riak_proxy = std::make_shared<rio::saltfish::RiakProxy>("localhost", 10017);
    rio::saltfish::SourceManagerServer sms(riak_proxy);
    server.register_service(&sms);
    cout << "Serving requests on port 5555." << endl;
    server.bind("tcp://127.0.0.1:5555");
    application.run();
    return 0;
}
