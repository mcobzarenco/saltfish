#include "sql.hpp"

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

MetadataSqlStore::MetadataSqlStore(
    const std::string& host, const uint16_t port, const std::string& user,
    const std::string& pass, const std::string& db, const bool thread_init_end)
    : host_{host}, port_{port}, user_{user}, pass_{pass}, db_{db},
      thread_init_end_{thread_init_end}, driver_{nullptr} {
}

bool MetadataSqlStore::ensure_connected() {
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

void MetadataSqlStore::close() {
  if(driver_ != nullptr && thread_init_end_) driver_->threadEnd();
  LOG(INFO) << "Closing SQL connection";
}

boost::optional<std::vector<std::string>> MetadataSqlStore::fetch_schema(
    const std::string& source_id) {
  static constexpr char GET_SOURCE_TEMPLATE[] =
      "SELECT source_id, user_id, source_schema, name FROM sources "
      "WHERE source_id = ?";
  if (!ensure_connected())  return boost::optional<std::vector<std::string>>{};
  try {
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
    return boost::optional<std::vector<std::string>>{std::vector<std::string>{}};
  } catch (const sql::SQLException& e) {
    LOG(WARNING) << "MetadataSqlStore::fetch_schema() - sql exception - "
                 << e.what();
  }
  return boost::optional<std::vector<std::string>>{};
}

boost::optional<int> MetadataSqlStore::create_source(
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
    LOG(WARNING) << "MetadataSqlStore::create_source() - sql exception - "
                 << e.what();
  }
  return boost::optional<int>{};
}

boost::optional<int> MetadataSqlStore::delete_source(
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
    LOG(WARNING) << "MetadataSqlStore::delete_source() - sql exception - "
                 << e.what();
  }
  return boost::optional<int>{};
}

boost::optional<std::vector<core::Source>>
MetadataSqlStore::list_sources(const int user_id) {
  static constexpr char GET_SOURCE_TEMPLATE[] =
      "SELECT source_id, user_id, source_schema, name FROM sources "
      "WHERE user_id = ?";
  if (!ensure_connected()) {
    return boost::optional<std::vector<core::Source>>{};
  }
  try {
    std::unique_ptr<sql::PreparedStatement> get_query{
      conn_->prepareStatement(GET_SOURCE_TEMPLATE)};
    get_query->setInt(1, user_id);
    std::unique_ptr<sql::ResultSet> res{get_query->executeQuery()};
    std::vector<core::Source> sources;

    LOG(INFO) << res->rowsCount() << " sources for used_id=" << user_id;
    while(res->next()) {
      core::Source src;
      if (!src.mutable_schema()->ParseFromString(
              res->getString("source_schema"))) {
        return boost::optional<std::vector<core::Source>>{};
      }
      src.set_source_id(res->getString("source_id"));
      src.set_user_id(res->getInt("user_id"));
      src.set_name(res->getString("name"));
      sources.push_back(src);
    }
    return boost::optional<std::vector<core::Source>>{sources};
  } catch (const sql::SQLException& e) {
    LOG(WARNING) << "MetadataSqlStore::list_sources() - sql exception - "
                 << e.what();
  }
  return boost::optional<std::vector<core::Source>>{};
}

MetadataSqlStoreTasklet::MetadataSqlStoreTasklet(
    zmq::context_t& context, const std::string& host, const uint16_t port,
    const std::string& user, const std::string& pass, const std::string& db)
    : store_{new MetadataSqlStore{host, port, user, pass, db, true}},
      tasklet_{context, []() { }, [&]() { store_.reset(nullptr); }} {
}

boost::optional<std::vector<std::string>>
MetadataSqlStoreTasklet::fetch_schema(const std::string& source_id) {
  using namespace std::placeholders;
  if (!fetch_schema_.get()) {
    fetch_schema_.reset(new lib::Connection<fetch_schema_type>{
        std::move(tasklet_.connect(fetch_schema_type(std::bind(
            &MetadataSqlStore::fetch_schema, store_.get(), _1))))});
  }
  return fetch_schema_->operator()(source_id);
}

boost::optional<int> MetadataSqlStoreTasklet::create_source(
    const std::string& source_id, int user_id,
    const std::string& schema, const std::string& name) {
  using namespace std::placeholders;
  if (!create_source_.get()) {
    create_source_.reset(new lib::Connection<create_source_type>{
        std::move(tasklet_.connect(create_source_type(std::bind(
            &MetadataSqlStore::create_source, store_.get(), _1, _2, _3, _4))))
            });
  }
  return create_source_->operator()(source_id, user_id, schema, name);
}

boost::optional<int> MetadataSqlStoreTasklet::delete_source(
    const std::string& source_id) {
  using namespace std::placeholders;
  if (!delete_source_.get()) {
    delete_source_.reset(new lib::Connection<delete_source_type>{
        std::move(tasklet_.connect(delete_source_type(std::bind(
            &MetadataSqlStore::delete_source, store_.get(), _1))))});
  }
  return delete_source_->operator()(source_id);
}

boost::optional<vector<core::Source>>
MetadataSqlStoreTasklet::list_sources(const int user_id) {
  using namespace std::placeholders;
  if (!list_sources_.get()) {
    list_sources_.reset(new lib::Connection<list_sources_type>{
        std::move(tasklet_.connect(list_sources_type(std::bind(
            &MetadataSqlStore::list_sources, store_.get(), _1))))});
  }
  return list_sources_->operator()(user_id);
}


}}}  // namespace reinferio::saltfish::store
