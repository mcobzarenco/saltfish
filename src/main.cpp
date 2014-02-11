#include "saltfish_config.hpp"
#include "server.hpp"

#include <boost/program_options.hpp>
#include <glog/logging.h>

#include <iostream>
#include <string>


using namespace std;
using namespace reinferio;

int main(int argc, char **argv) {
  namespace po = boost::program_options;
  google::InitGoogleLogging(argv[0]);
  google::LogToStderr();

  auto build_time = string{__TIMESTAMP__};
  string conf_file;
  string bind_str;
  string riak_host;
  uint16_t riak_port{0};
  auto description = po::options_description{
    "Saltfish server (built on " + build_time +
    ") manages schemas and data for soruces.\n\nAllowed options:"
  };
  description.add_options()
      ("help,h", "prints this help message")
      ("conf",
       po::value<string>(&conf_file)->value_name("FILE"),
       "Config file. Options in cmd line overwrite the values from the file.")
      ("bind",
       po::value<string>(&bind_str)->value_name("STR"),
       "ZeroMQ bind string")
      ("riak-host",
       po::value<string>(&riak_host)->value_name("HOST"),
       "Riak node hostname")
      ("riak-port",
       po::value<uint16_t>(&riak_port)->value_name("PORT"),
       "Riak node port (pbc protocol)");

  auto variables = po::variables_map{};
  try {
    po::store(po::parse_command_line(argc, argv, description), variables);
    po::notify(variables);
    if (variables.count("help")) {
      cerr << description << endl;
      return 0;
    }
    saltfish::config::Saltfish conf;
    if (!conf_file.empty())   conf = saltfish::parse_config_file(conf_file);
    if (!bind_str.empty())    conf.set_bind_str(bind_str);
    if (!riak_host.empty())   conf.mutable_riak()->set_host(riak_host);
    if (riak_port != 0)       conf.mutable_riak()->set_port(riak_port);

    saltfish::SaltfishServer server(conf);
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
