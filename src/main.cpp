#include "server.hpp"

#include <boost/program_options.hpp>
#include <glog/logging.h>

#include <iostream>
#include <string>


using namespace std;
using namespace reinferio;


const string DEFAULT_BIND_STR = "tcp://127.0.0.1:5555";
const string DEFAULT_RIAK_HOST = "127.0.0.1";
const uint16_t DEFAULT_RIAK_PORT = 10017;


int main(int argc, char **argv) {
  namespace po = boost::program_options;
  google::InitGoogleLogging(argv[0]);
  google::LogToStderr();

  auto build_time = string{__TIMESTAMP__};
  auto bind_str = string{DEFAULT_BIND_STR};
  auto riak_host = string{DEFAULT_RIAK_HOST};
  auto riak_port = uint16_t{DEFAULT_RIAK_PORT};

  auto description = po::options_description{
    "Saltfish server (built on " + build_time +") manages \n\nAllowed options:"
  };
  description.add_options()
      ("help,h", "produce help message")
      ("bind", po::value<string>(&bind_str),
       ("ZeroMQ bind string; default=" + bind_str).c_str())
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

    saltfish::SaltfishServer server(bind_str, riak_host, riak_port);
    server.run();

  } catch (const boost::program_options::unknown_option& e) {
    LOG(ERROR) << e.what();
    return 1;
  } catch (const boost::program_options::invalid_option_value& e) {
    LOG(ERROR) << e.what();
    return 2;
  } catch(const std::exception& e) {
    LOG(ERROR) << e.what();
    return -1;
  }

  return 0;
}
