#ifndef REINFERIO_SALTFISH_SQL_POOL_HPP
#define REINFERIO_SALTFISH_SQL_POOL_HPP

#define MYSQLPP_MYSQL_HEADERS_BURIED
#include <mysql++/mysql++.h>

#include <string>


namespace reinferio {
namespace saltfish {
namespace sql {

// TODO: Implement our own connection pool
// Do not use the abstract base class as it does old style C++ evil things
class ConnectionPool : public mysqlpp::ConnectionPool {
 public:
  ConnectionPool(const std::string& host, const std::string& db,
                 const std::string& user, const std::string& password);
  ~ConnectionPool();

  ConnectionPool(const ConnectionPool&) = delete;
  ConnectionPool& operator=(const ConnectionPool&) = delete;

 protected:
  mysqlpp::Connection* create() override;
  void destroy(mysqlpp::Connection* conn) override;
  unsigned int max_idle_time() override;

 private:
  const std::string host_;
  const std::string db_;
  const std::string user_;
  const std::string password_;
};

}  // namespace sql
}  // namespace saltfish
}  // namespace reinferio

#endif  // REINFERIO_SALTFISH_SQL_POOL_HPP
