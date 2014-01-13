#ifndef REINFERIO_SALTFISH_SQL_POOL_HPP
#define REINFERIO_SALTFISH_SQL_POOL_HPP

#include <memory>
#include <mutex>
#include <string>


namespace sql {
class Driver;
class Connection;
}

namespace reinferio { namespace saltfish { namespace sql {

class ConnectionFactory {
  public:
    ConnectionFactory(const std::string& host, const std::string& user,
                      const std::string& pass, const std::string& db);

    std::unique_ptr<::sql::Connection> new_connection();
  private:
    std::mutex driver_mutex_;
    ::sql::Driver* driver_;
    const std::string host_;
    const std::string user_;
    const std::string pass_;
    const std::string db_;
};

}}}  // namespace reinferio::saltfish::sql

#endif  // REINFERIO_SALTFISH_SQL_POOL_HPP
