#include "sql_pool.hpp"

#include <glog/logging.h>
#include <cppconn/exception.h>
#include <cppconn/statement.h>
#include <mysql_driver.h>
#include <mysql_connection.h>


namespace reinferio { namespace saltfish { namespace sql {

using namespace std;

ConnectionFactory::ConnectionFactory(
    const std::string& host, const std::string& user,
    const std::string& pass, const std::string& db)
    : driver_{::sql::mysql::get_driver_instance()}, host_{host},
               user_{user}, pass_{pass}, db_{db} {
}

unique_ptr< ::sql::Connection > ConnectionFactory::new_connection() {
  lock_guard<mutex> guard{driver_mutex_};
  unique_ptr< ::sql::Connection > conn{ driver_->connect(host_, user_, pass_) };
  conn->setSchema(db_);
  return move(conn);
}

}}}  // namespace reinferio::saltfish::sql
