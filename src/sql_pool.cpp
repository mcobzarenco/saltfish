#include "sql_pool.hpp"

#include <glog/logging.h>
#include <cppconn/exception.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>
#include <mysql_driver.h>
#include <mysql_connection.h>

#include <thread>


namespace reinferio { namespace saltfish { namespace store {

using namespace std;
using namespace std::placeholders;

ConnectionFactory::ConnectionFactory(
    const std::string& host, const std::string& user,
    const std::string& pass, const std::string& db)
    : driver_{::sql::mysql::get_driver_instance()}, host_{host},
  user_{user}, pass_{pass}, db_{db} {
  }

unique_ptr<sql::Connection > ConnectionFactory::new_connection() {
  lock_guard<mutex> guard{driver_mutex_};
  unique_ptr< ::sql::Connection > conn{ driver_->connect(host_, user_, pass_) };
  conn->setSchema(db_);
  return move(conn);
}

std::unique_ptr<sql::Connection> connect_to_sql(
    const std::string& host, const std::string& user,
    const std::string& pass, const std::string& db) {
  sql::Driver* driver_ = ::sql::mysql::get_driver_instance();
  unique_ptr<sql::Connection> conn{ driver_->connect(host, user, pass)};
  conn->setSchema(db);
  return conn;
}

SourceMetadataSqlStore::SourceMetadataSqlStore(
    const std::string& host, const std::string& user,
    const std::string& pass, const std::string& db,
    bool thread_init_end)
  : host_{host}, user_{user}, pass_{pass}, db_{db},
    thread_init_end_{thread_init_end}, connected_{false}, driver_{nullptr} {
}

void SourceMetadataSqlStore::connect() {
  driver_ = sql::mysql::get_driver_instance();
  CHECK(driver_) << "Could not retrieve MySQL driver";
  if(thread_init_end_) driver_->threadInit();
  conn_.reset(driver_->connect(host_, user_, pass_));
  conn_->setSchema(db_);
  connected_ = true;
}

void SourceMetadataSqlStore::close() {
  if(thread_init_end_) driver_->threadEnd();
  LOG(INFO) << "closing sql conn..";
}

boost::optional<std::string> SourceMetadataSqlStore::fetch_schema(
    const std::string& source_id) {
  static constexpr char GET_SOURCE_TEMPLATE[] =
      "SELECT source_id, user_id, source_schema, name FROM sources "
      "WHERE source_id = ?";

  CHECK(connected_) << "Not connected to SQL store";
  std::unique_ptr<sql::PreparedStatement> get_query{
    conn_->prepareStatement(GET_SOURCE_TEMPLATE)};
  get_query->setString(1, source_id);
  std::unique_ptr<sql::ResultSet> res{get_query->executeQuery()};
  if (res->rowsCount() > 0) {
    CHECK(res->rowsCount() == 1)
        << "Integrity constraint violated, source_id is a primary key";
    VLOG(0) << "source_id already exists";
    res->next();
    return boost::optional<std::string>{res->getString("source_schema")};
  }
  return boost::optional<std::string>();
}

int SourceMetadataSqlStore::create_source(
    const std::string& source_id, int user_id,
    const std::string& schema, const std::string& name) {
  static constexpr char CREATE_SOURCE_TEMPLATE[] =
      "INSERT INTO sources (source_id, user_id, source_schema, name) "
      "VALUES (?, ?, ?, ?)";

  CHECK(connected_) << "Not connected to SQL store";
  unique_ptr<sql::PreparedStatement> query{
    conn_->prepareStatement(CREATE_SOURCE_TEMPLATE)};
  query->setString(1, source_id);
  query->setInt(2, user_id);
  query->setString(3, schema);
  query->setString(4, name);
  return query->executeUpdate();
}

int SourceMetadataSqlStore::delete_source(const std::string& source_id) {
  static constexpr char DELETE_SOURCE_TEMPLATE[] =
      "DELETE FROM sources WHERE source_id = ?";

  CHECK(connected_) << "Not connected to SQL store";
  unique_ptr<sql::PreparedStatement> query{
    conn_->prepareStatement(DELETE_SOURCE_TEMPLATE)};
  query->setString(1, source_id);
  auto rows_updated = query->executeUpdate();
  CHECK(rows_updated == 0 || rows_updated == 1)
      << "source_id is a primary key, a max of 1 row can be affected";
  return rows_updated;
}

SourceMetadataSqlStoreTasklet::SourceMetadataSqlStoreTasklet(
    zmq::context_t& context, const std::string& host, const std::string& user,
    const std::string& pass, const std::string& db)
    : store_{host, user, pass, db, true},
      tasklet_{context, [&]() { store_.connect(); }, [&]() { store_.close(); }} {
}

boost::optional<std::string> SourceMetadataSqlStoreTasklet::fetch_schema(
    const std::string& source_id) {
  using namespace std::placeholders;
  if (!fetch_schema_.get()) {
    fetch_schema_.reset(new lib::Connection<fetch_schema_type>{
        std::move(tasklet_.connect(fetch_schema_type(std::bind(
            &SourceMetadataSqlStore::fetch_schema, &store_, _1))))});
  }
  return fetch_schema_->operator()(source_id);
}

int SourceMetadataSqlStoreTasklet::create_source(
    const std::string& source_id, int user_id,
    const std::string& schema, const std::string& name) {
  using namespace std::placeholders;
  if (!create_source_.get()) {
    create_source_.reset(new lib::Connection<create_source_type>{
        std::move(tasklet_.connect(create_source_type(std::bind(
            &SourceMetadataSqlStore::create_source, &store_, _1, _2, _3, _4))))
            });
  }
  return create_source_->operator()(source_id, user_id, schema, name);
}

int SourceMetadataSqlStoreTasklet::delete_source(const std::string& source_id) {
  using namespace std::placeholders;
  if (!delete_source_.get()) {
    delete_source_.reset(new lib::Connection<delete_source_type>{
        std::move(tasklet_.connect(delete_source_type(std::bind(
            &SourceMetadataSqlStore::delete_source, &store_, _1))))});
  }
  return delete_source_->operator()(source_id);
}

}}}  // namespace reinferio::saltfish::store
