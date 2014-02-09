#include "sql_pool.hpp"

#include <glog/logging.h>
#include <cppconn/exception.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>
#include <mysql_driver.h>
#include <mysql_connection.h>

#include <functional>


namespace reinferio { namespace saltfish { namespace store {

namespace {
constexpr uint32_t SQL_CONNECT_MAX_RETRIES = 3;
}

using namespace std;
using namespace std::placeholders;

std::unique_ptr<sql::Connection> connect_to_sql(
    const std::string& host, const std::string& user,
    const std::string& pass, const std::string& db) {
  sql::Driver* driver_ = ::sql::mysql::get_driver_instance();
  unique_ptr<sql::Connection> conn{ driver_->connect(host, user, pass)};
  conn->setSchema(db);
  return conn;
}

SourceMetadataSqlStore::SourceMetadataSqlStore(
    const std::string& host, const uint16_t port, const std::string& user,
    const std::string& pass, const std::string& db, const bool thread_init_end)
    : host_{host}, port_{port}, user_{user}, pass_{pass}, db_{db},
      thread_init_end_{thread_init_end}, driver_{nullptr} {
}

bool SourceMetadataSqlStore::ensure_connected() {
  if (!driver_) {
    driver_ = sql::mysql::get_driver_instance();
    CHECK(driver_) << "Could not retrieve MySQL driver";
    if(thread_init_end_) driver_->threadInit();
  }
  uint32_t n_retry{0};
  sql::ConnectOptionsMap properties;
  properties["hostName"] = host_;
  properties["port"] = port_;
  properties["userName"] = user_;
  properties["password"] = pass_;
  while (true) {
    if (n_retry++ >= SQL_CONNECT_MAX_RETRIES)  return false;
    try {
      conn_.reset(driver_->connect(properties));
      conn_->setSchema(db_);
      return true;
    } catch (const sql::SQLException& e) {
      LOG(WARNING) << "[retry " << n_retry << "] " << e.what();
      conn_.reset();
    }
  }
}

void SourceMetadataSqlStore::close() {
  if(driver_ != nullptr && thread_init_end_) driver_->threadEnd();
  LOG(INFO) << "Closing SQL connection";
}

boost::optional<std::vector<std::string>> SourceMetadataSqlStore::fetch_schema(
    const std::string& source_id) {
  static constexpr char GET_SOURCE_TEMPLATE[] =
      "SELECT source_id, user_id, source_schema, name FROM sources "
      "WHERE source_id = ?";
  if (!ensure_connected())  return boost::optional<std::vector<std::string>>{};
  std::unique_ptr<sql::PreparedStatement> get_query{
    conn_->prepareStatement(GET_SOURCE_TEMPLATE)};
  get_query->setString(1, source_id);
  std::unique_ptr<sql::ResultSet> res{get_query->executeQuery()};
  if (res->rowsCount() > 0) {
    CHECK(res->rowsCount() == 1)
        << "Integrity constraint violated, source_id is a primary key";
    VLOG(0) << "source_id already exists";
    res->next();
    return boost::optional<std::vector<std::string>>{
      std::vector<string>{{res->getString("source_schema")}}};
  }
  return boost::optional<std::vector<std::string>>{};
}

boost::optional<int> SourceMetadataSqlStore::create_source(
    const std::string& source_id, int user_id,
    const std::string& schema, const std::string& name) {
  static constexpr char CREATE_SOURCE_TEMPLATE[] =
      "INSERT INTO sources (source_id, user_id, source_schema, name) "
      "VALUES (?, ?, ?, ?)";
  if (!ensure_connected())  return boost::optional<int>{};
  try {
    unique_ptr<sql::PreparedStatement> query{
      conn_->prepareStatement(CREATE_SOURCE_TEMPLATE)};
    query->setString(1, source_id);
    query->setInt(2, user_id);
    query->setString(3, schema);
    query->setString(4, name);
    return boost::optional<int>{query->executeUpdate()};
  } catch (const sql::SQLException& e) {
    LOG(WARNING) << "SourceMetadataSqlStore::create_source() - sql exception - "
                 << e.what();
  }
  return boost::optional<int>{};
}

boost::optional<int> SourceMetadataSqlStore::delete_source(
    const std::string& source_id) {
  static constexpr char DELETE_SOURCE_TEMPLATE[] =
      "DELETE FROM sources WHERE source_id = ?";
  if (!ensure_connected())  return boost::optional<int>{};
  try {
    unique_ptr<sql::PreparedStatement> query{
      conn_->prepareStatement(DELETE_SOURCE_TEMPLATE)};
    query->setString(1, source_id);
    auto rows_updated = query->executeUpdate();
    CHECK(rows_updated == 0 || rows_updated == 1)
        << "source_id is a primary key, a max of 1 row can be affected";
    return boost::optional<int>{rows_updated};
  } catch (const sql::SQLException& e) {
    LOG(WARNING) << "SourceMetadataSqlStore::delete_source() - sql exception - "
                 << e.what();
  }
  return boost::optional<int>{};
}

SourceMetadataSqlStoreTasklet::SourceMetadataSqlStoreTasklet(
    zmq::context_t& context, const std::string& host, const uint16_t port,
    const std::string& user, const std::string& pass, const std::string& db)
    : store_{new SourceMetadataSqlStore{host, port, user, pass, db, true}},
      tasklet_{context, []() { }, [&]() { store_.reset(nullptr); }} {
}

boost::optional<std::vector<std::string>> SourceMetadataSqlStoreTasklet::fetch_schema(
    const std::string& source_id) {
  using namespace std::placeholders;
  if (!fetch_schema_.get()) {
    fetch_schema_.reset(new lib::Connection<fetch_schema_type>{
        std::move(tasklet_.connect(fetch_schema_type(std::bind(
            &SourceMetadataSqlStore::fetch_schema, store_.get(), _1))))});
  }
  return fetch_schema_->operator()(source_id);
}

boost::optional<int> SourceMetadataSqlStoreTasklet::create_source(
    const std::string& source_id, int user_id,
    const std::string& schema, const std::string& name) {
  using namespace std::placeholders;
  if (!create_source_.get()) {
    create_source_.reset(new lib::Connection<create_source_type>{
        std::move(tasklet_.connect(create_source_type(std::bind(
            &SourceMetadataSqlStore::create_source, store_.get(), _1, _2, _3, _4))))
            });
  }
  return create_source_->operator()(source_id, user_id, schema, name);
}

boost::optional<int> SourceMetadataSqlStoreTasklet::delete_source(
    const std::string& source_id) {
  using namespace std::placeholders;
  if (!delete_source_.get()) {
    delete_source_.reset(new lib::Connection<delete_source_type>{
        std::move(tasklet_.connect(delete_source_type(std::bind(
            &SourceMetadataSqlStore::delete_source, store_.get(), _1))))});
  }
  return delete_source_->operator()(source_id);
}

}}}  // namespace reinferio::saltfish::store
