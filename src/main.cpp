#include "saltfish_config.hpp"
#include "server.hpp"

#include <boost/program_options.hpp>
#include <glog/logging.h>

#include <iostream>
#include <string>


using namespace std;
using namespace reinferio;

namespace {
  // Defaults for command line arguments
  constexpr const char* DEFAULT_BIND_STRING{"tcp://127.0.0.1:5555"};
  constexpr const char* DEFAULT_RECORDS_BUCKET_PREFIX{"sources:records"};
  constexpr const char* DEFAULT_SCHEMAS_BUCKET{"sources:schemas"};

  constexpr const char* DEFAULT_RIAK_HOSTNAME{"localhost"};
  constexpr uint16_t    DEFAULT_RIAK_PORT{8087};

  constexpr const char* DEFAULT_MARIADB_HOSTNAME{"localhost"};
  constexpr uint16_t    DEFAULT_MARIADB_PORT{3306};
  constexpr const char* DEFAULT_MARIADB_DB{"mlaas"};
  constexpr const char* DEFAULT_MARIADB_USER{"super"};

  constexpr const char* DEFAULT_REDIS_HOSTNAME{"localhost"};
  constexpr uint16_t    DEFAULT_REDIS_PORT{6379};
  constexpr const char* DEFAULT_REDIS_PUBKEY{"saltfish:pub"};

  // Name of command line arguments:
  constexpr const char* ARG_CONFIG{"config"};
  constexpr const char* ARG_BIND_STRING{"bind"};
  constexpr const char* ARG_RECORDS_BUCKET_PREFIX{"records-prefix"};
  constexpr const char* ARG_SCHEMAS_BUCKET{"schemas-bucket"};

  constexpr const char* ARG_RIAK_HOSTNAME{"riak-host"};
  constexpr const char* ARG_RIAK_PORT{"riak-port"};

  constexpr const char* ARG_MARIADB_HOSTNAME{"sql-host"};
  constexpr const char* ARG_MARIADB_PORT{"sql-port"};
  constexpr const char* ARG_MARIADB_DB{"sql-db"};
  constexpr const char* ARG_MARIADB_USER{"sql-user"};
  constexpr const char* ARG_MARIADB_PASSWORD{"sql-password"};

  constexpr const char* ARG_REDIS_HOSTNAME{"redis-host"};
  constexpr const char* ARG_REDIS_PORT{"redis-port"};
  constexpr const char* ARG_REDIS_PUBKEY{"redis-pubkey"};
}

int main(int argc, char **argv) {
  namespace po = boost::program_options;
  google::InitGoogleLogging(argv[0]);
  google::LogToStderr();

  auto build_time = string{__TIMESTAMP__};
  string config_file;
  string bind_str, records_bucket_prefix, schemas_bucket;

  string riak_host;
  uint16_t riak_port{0};

  string sql_host, sql_db, sql_user, sql_password;
  uint16_t sql_port{0};

  string redis_host, redis_pubkey;
  uint16_t redis_port{0};

  auto description = po::options_description{
    "Saltfish server (built on " + build_time +
    ") manages schemas and data for soruces.\n\nAllowed options:"
  };
  description.add_options()
    ("help,h", "Prints this help message.")
    (ARG_CONFIG, po::value<string>(&config_file)
     ->value_name("FILE"),
     "Read options from config file. "
     "Options in command line overwrite the values from the file.")
    (ARG_BIND_STRING, po::value<string>(&bind_str)
     ->value_name("BIND")->default_value(DEFAULT_BIND_STRING),
     "Where to bind - ZeroMQ bind string format")
    (ARG_RECORDS_BUCKET_PREFIX, po::value<string>(&records_bucket_prefix)
     ->value_name("PREFIX")->default_value(DEFAULT_RECORDS_BUCKET_PREFIX),
     "Prefix for Riak buckets where records are stored."
     "The actual bucket = prefix + base64 encoded source id")
    (ARG_SCHEMAS_BUCKET, po::value<string>(&schemas_bucket)
     ->value_name("BUCKET")->default_value(DEFAULT_SCHEMAS_BUCKET),
     "Riak bucket where to cache schemas for sources.")

    (ARG_RIAK_HOSTNAME, po::value<string>(&riak_host)
     ->value_name("HOST")->default_value(DEFAULT_RIAK_HOSTNAME),
     "Riak node hostname")
    (ARG_RIAK_PORT, po::value<uint16_t>(&riak_port)
     ->value_name("PORT")->default_value(DEFAULT_RIAK_PORT),
     "Riak node port (pbc protocol)")

    (ARG_MARIADB_HOSTNAME, po::value<string>(&sql_host)
     ->value_name("HOST")->default_value(DEFAULT_MARIADB_HOSTNAME),
     "MariaDB hostname")
    (ARG_MARIADB_PORT, po::value<uint16_t>(&sql_port)
     ->value_name("PORT")->default_value(DEFAULT_MARIADB_PORT),
     "MariaDB port")
    (ARG_MARIADB_DB, po::value<string>(&sql_db)
     ->value_name("DB")->default_value(DEFAULT_MARIADB_DB),
     "MariaDB database")
    (ARG_MARIADB_USER, po::value<string>(&sql_user)
     ->value_name("USER")->default_value(DEFAULT_MARIADB_USER),
     "MariaDB user")
    (ARG_MARIADB_PASSWORD, po::value<string>(&sql_password)
     ->value_name("PASS"),
     "Password for MariaDB user")

    (ARG_REDIS_HOSTNAME, po::value<string>(&redis_host)
     ->value_name("HOST")->default_value(DEFAULT_REDIS_HOSTNAME),
     "Redis hostname")
    (ARG_REDIS_PORT, po::value<uint16_t>(&redis_port)
     ->value_name("PORT")->default_value(DEFAULT_REDIS_PORT),
     "Redis port")
    (ARG_REDIS_PUBKEY, po::value<string>(&redis_pubkey)
     ->value_name("KEY")->default_value(DEFAULT_REDIS_PUBKEY),
     "Key where events are published via Redis pubsub");

  auto variables = po::variables_map{};
  try {
    po::store(po::parse_command_line(argc, argv, description), variables);
    po::notify(variables);
    if (variables.count("help")) {
      cerr << description << endl;
      return 0;
    }
    saltfish::config::Saltfish conf;
    if (variables.count(ARG_CONFIG))
      conf = saltfish::parse_config_file(config_file);
    if (!conf.has_bind_str() ||
        !variables[ARG_BIND_STRING].defaulted()) {
      conf.set_bind_str(bind_str);
    }
    if (!conf.has_sources_data_bucket_prefix() ||
        !variables[ARG_RECORDS_BUCKET_PREFIX].defaulted()) {
      conf.set_sources_data_bucket_prefix(records_bucket_prefix);
    }
    if (!conf.has_schemas_bucket() ||
        !variables[ARG_SCHEMAS_BUCKET].defaulted()) {
      conf.set_schemas_bucket(schemas_bucket);
    }

    // Arguments relating to Riak:
    if (!conf.riak().has_host() ||
        !variables[ARG_RIAK_HOSTNAME].defaulted()) {
      conf.mutable_riak()->set_host(riak_host);
    }
    if (!conf.riak().has_port() ||
        !variables[ARG_RIAK_PORT].defaulted()) {
      conf.mutable_riak()->set_port(riak_port);
    }

    // Arguments relating to Sql:
    if (!conf.maria_db().has_host() ||
        !variables[ARG_MARIADB_HOSTNAME].defaulted()) {
      conf.mutable_maria_db()->set_host(sql_host);
    }
    if (!conf.maria_db().has_port() ||
        !variables[ARG_MARIADB_PORT].defaulted()) {
      conf.mutable_maria_db()->set_port(sql_port);
    }
    if (!conf.maria_db().has_db() ||
        !variables[ARG_MARIADB_DB].defaulted()) {
      conf.mutable_maria_db()->set_db(sql_db);
    }
    if (!conf.maria_db().has_user() ||
        !variables[ARG_MARIADB_USER].defaulted()) {
      conf.mutable_maria_db()->set_user(sql_user);
    }
    if (variables.count(ARG_MARIADB_PASSWORD)) {
      conf.mutable_maria_db()->set_password(sql_password);
    }

    // Arguments relating to Redis:
    if (!conf.redis().has_host() ||
        !variables[ARG_REDIS_HOSTNAME].defaulted()) {
      conf.mutable_redis()->set_host(redis_host);
    }
    if (!conf.redis().has_port() ||
        !variables[ARG_REDIS_PORT].defaulted()) {
      conf.mutable_redis()->set_port(redis_port);
    }
    if (!conf.redis().has_key() ||
        !variables[ARG_REDIS_PUBKEY].defaulted()) {
      conf.mutable_redis()->set_key(redis_pubkey);
    }

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
