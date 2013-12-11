#ifndef REINFERIO_SALTFISH_SALTFISH_CONFIG_HPP
#define REINFERIO_SALTFISH_SALTFISH_CONFIG_HPP

#include "config.pb.h"

#include <google/protobuf/text_format.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include <fstream>
#include <string>
#include <stdexcept>


using namespace std;


namespace reinferio {
namespace saltfish {

class BadConfigFile : public std::runtime_error {
 public:
  BadConfigFile(const std::string& msg) : runtime_error(msg) {}
};

config::Saltfish parse_config_file(const std::string& file_path) {
  std::ifstream file;
  file.exceptions (std::ifstream::badbit);
  file.open(file_path);
  if (!file.is_open()) {
    throw BadConfigFile("Could not open the configuration file: " + file_path);
  }

  google::protobuf::io::IstreamInputStream proto_stream(&file);
  config::Saltfish conf;
  if (!google::protobuf::TextFormat::Parse(&proto_stream, &conf)) {
    throw BadConfigFile(file_path + " is not well formed");
  }
  return std::move(conf);
}

}  // saltfish
}  // reinferio

#endif  // REINFERIO_SALTFISH_SALTFISH_CONFIG_HPP
