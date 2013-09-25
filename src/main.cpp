#include "service.pb.h"
#include "service.rpcz.h"

#include <rpcz/rpcz.hpp>

#include <iostream>


using namespace std;

namespace rio {
namespace saltfish {


class SourceManagerServer : public SourceManager {
    virtual void put_row(const source::SourceRow& request, rpcz::reply<saltfish::Response> response) {
    }

    // virtual void execute(const ::Query& request, rpcz::reply<Query> response) {
    // 	cout << "Got: \'" << request.query() << "\'" << endl;
    // 	Query query;
    // 	query.set_query(request.query());
    // 	response.send(query);
    // }

};


}  // namespace rio
}  // namespace saltfish


int main() {
    rpcz::application> application;
    rpcz::server server(application);
    rio::saltfish::SourceManagerServer sms;
    server.register_service(&sms);
    cout << "Serving requests on port 5555." << endl;
    server.bind("tcp://*:5555");
    application.run();
    return 0;
}
