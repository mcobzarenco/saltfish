#include "server.hpp"

#include <boost/program_options.hpp>
#include <glog/logging.h>

#include <iostream>
#include <string>
#include <csignal>


using namespace std;
using namespace reinferio;


int main(int argc, char **argv) {
  namespace po = boost::program_options;
  google::InitGoogleLogging(argv[0]);
  google::LogToStderr();

  auto bind_str = string{"tcp://127.0.0.1:5555"};
  auto riak_host = string{"127.0.0.1"};
  auto riak_port = uint16_t{10017};

  auto description = po::options_description{"Allowed options"};
  description.add_options()
      ("help,h", "produce help message")
      ("bind", po::value<string>(&bind_str),
       "ZeroMQ bind string; default=tcp://127.0.0.1:5555")
      ("riak-host", po::value<string>(&riak_host),
       "hostname of a Riak node")
      ("riak-port", po::value<uint16_t>(&riak_port),
       "what port to use to connect to Riak (pbc protocol)");

  auto variables = po::variables_map{};
  try {
    po::store(po::parse_command_line(argc, argv, description), variables);
    po::notify(variables);
    if (variables.count("help")) {
      cerr << description << endl;
      return 0;
    }
  } catch (const boost::program_options::unknown_option& e) {
    LOG(ERROR) << e.what();
    return 1;
  } catch (const boost::program_options::invalid_option_value& e) {
    LOG(ERROR) << e.what();
    return 2;
  }

  // saltfish::SaltfishServer server(bind_str, riak_host, riak_port);
  shared_ptr<saltfish::SaltfishServer> server =
      saltfish::SaltfishServer::create_server(bind_str, riak_host, riak_port);
  server->run();
  return 0;
}
