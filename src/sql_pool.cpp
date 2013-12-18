#include "sql_pool.hpp"

#include <glog/logging.h>


namespace reinferio {
namespace saltfish {
namespace sql {

using namespace std;

ConnectionPool::ConnectionPool(const string& host, const string& db,
                               const string& user, const string& password) :
    host_(host), db_(db), user_(user), password_(password) {
  mysqlpp::ScopedConnection conn(*this, true);
  CHECK(conn->thread_aware())
    << "MySQL++ wasn't built with thread awareness. Cannot run without it.";
}

// The destructor.  We _must_ call ConnectionPool::clear() here,
// because our superclass can't do it for us.
ConnectionPool::~ConnectionPool() {
  clear();
}

mysqlpp::Connection* ConnectionPool::create() {
  // Create connection using the parameters we were passed upon
  // creation.  This could be something much more complex, but for
  // the purposes of the example, this suffices.
  LOG(INFO) << "create()" << endl;
  return new mysqlpp::Connection(db_.c_str(), host_.c_str(),
                                 user_.c_str(), password_.c_str());
}

void ConnectionPool::destroy(mysqlpp::Connection* conn) {
  // Our superclass can't know how we created the Connection, so
  // it delegates destruction to us, to be safe.
  LOG(INFO) << "destroy()" << endl;
  conn->disconnect();  //TODO: does it throw?
  delete conn;
}

unsigned int ConnectionPool::max_idle_time() {
  // Set our idle time at an example-friendly 3 seconds.  A real
  // pool would return some fraction of the server's connection
  // idle timeout instead.
  return 3;
}

}  // namespace sql
}  // namespace saltfish
}  // namespace reinferio
