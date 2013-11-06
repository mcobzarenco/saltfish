#include "server.hpp"

#include <boost/program_options.hpp>
#include <glog/logging.h>

#include <iostream>
#include <string>


using namespace std;
using namespace reinferio;


namespace {
const char DEFAULT_BIND_STR[]{"tcp://127.0.0.1:5555"};
const char DEFAULT_RIAK_HOST[]{"127.0.0.1"};
const uint16_t DEFAULT_RIAK_PORT{10017};
}


int main(int argc, char **argv) {
  namespace po = boost::program_options;
  google::InitGoogleLogging(argv[0]);
  google::LogToStderr();

  auto build_time = string{__TIMESTAMP__};
  auto bind_str = string{};
  auto riak_host = string{DEFAULT_RIAK_HOST};
  auto riak_port = uint16_t{DEFAULT_RIAK_PORT};

  auto description = po::options_description{
    "Saltfish server (built on " + build_time +
    ") manages schemas and data for soruces.\n\nAllowed options:"
  };
  description.add_options()
      ("help,h", "prints this help message")
      ("bind",
       po::value<string>(&bind_str)
         ->default_value(DEFAULT_BIND_STR)->value_name("STR"),
       "ZeroMQ bind string")
      ("riak-host",
       po::value<string>(&riak_host)
         ->default_value(DEFAULT_RIAK_HOST)->value_name("HOST"),
       "Riak node hostname")
      ("riak-port",
        po::value<uint16_t>(&riak_port)
          ->default_value(DEFAULT_RIAK_PORT)->value_name("PORT"),
       "Riak node port (pbc protocol)");

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
