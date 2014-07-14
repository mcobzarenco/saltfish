#include "sql.hpp"
#include "sql_errors.hpp"
#include "service_utils.hpp"

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

std::error_condition MetadataSqlStore::fetch_schema(
    core::Schema& schema, const std::string& source_id) {
  static constexpr char GET_SOURCE_TEMPLATE[] =
      "SELECT source_id, user_id, source_schema, name FROM sources "
      "WHERE source_id = ?";
  if (!ensure_connected()) {
    return make_error_condition(SqlErr::SQL_CONNECTION_ERROR);
  }
  try {
    std::unique_ptr<sql::PreparedStatement> get_query{
      conn_->prepareStatement(GET_SOURCE_TEMPLATE)};
    get_query->setString(1, source_id);
    std::unique_ptr<sql::ResultSet> res{get_query->executeQuery()};
    if (res->rowsCount() > 0) {
      CHECK(res->rowsCount() == 1)
          << "Integrity constraint violated, source_id is a primary key";
      res->next();
      CHECK(schema.ParseFromString(res->getString("source_schema")))
          << "Could not parse the source schema for source_id="
          << b64encode(source_id);
      return make_error_condition(SqlErr::OK);
    }
    return make_error_condition(SqlErr::INVALID_SOURCE_ID);
  } catch (const sql::SQLException& e) {
    LOG(WARNING) << "MetadataSqlStore::fetch_schema() - sql exception - "
                 << e.what();
  }
  return make_error_condition(SqlErr::SQL_CONNECTION_ERROR);
}

std::error_condition MetadataSqlStore::create_source(
    const std::string& source_id, int user_id,
    const std::string& schema, const std::string& name,
    bool private_, bool frozen) {
  static constexpr char CREATE_SOURCE_TEMPLATE[] =
      "INSERT INTO sources (source_id, user_id, source_schema, name, "
      "private, frozen) VALUES (?, ?, ?, ?, ?, ?)";
  if (!ensure_connected()) {
    return make_error_condition(SqlErr::SQL_CONNECTION_ERROR);
  }
  try {
    unique_ptr<sql::PreparedStatement> query{
      conn_->prepareStatement(CREATE_SOURCE_TEMPLATE)};
    query->setString(1, source_id);
    query->setInt(2, user_id);
    query->setString(3, schema);
    query->setString(4, name);
    query->setBoolean(5, private_);
    query->setBoolean(6, frozen);
    query->executeUpdate();
    return make_error_condition(SqlErr::OK);
  } catch (const sql::SQLException& e) {
    string msg{e.what()};
    LOG(WARNING) << "MetadataSqlStore::create_source() - sql exception - "
                 << msg;
    if (msg.find("sources_user_name") != string::npos) {
      return make_error_condition(SqlErr::DUPLICATE_SOURCE_NAME);
    } else if (msg.find("FOREIGN KEY (`user_id`)") != string::npos) {
      return make_error_condition(SqlErr::INVALID_USER_ID);
    }
  }
  return make_error_condition(SqlErr::SQL_CONNECTION_ERROR);
}

std::error_condition MetadataSqlStore::delete_source(
    int& rows_updated, const std::string& source_id) {
  static constexpr char DELETE_SOURCE_TEMPLATE[] =
      "DELETE FROM sources WHERE source_id = ?";
  if (!ensure_connected()) {
    return make_error_condition(SqlErr::SQL_CONNECTION_ERROR);
  }
  try {
    unique_ptr<sql::PreparedStatement> query{
      conn_->prepareStatement(DELETE_SOURCE_TEMPLATE)};
    query->setString(1, source_id);
    rows_updated = query->executeUpdate();
    CHECK(rows_updated == 0 || rows_updated == 1)
        << "source_id is a primary key, a max of 1 row can be affected";
    return make_error_condition(SqlErr::OK);
  } catch (const sql::SQLException& e) {
    LOG(WARNING) << "MetadataSqlStore::delete_source() - sql exception - "
                 << e.what();
  }
  return make_error_condition(SqlErr::SQL_CONNECTION_ERROR);
}

namespace {
void list_sources_row_to_proto(
    SourceInfo& source_info, sql::ResultSet& row) {
  core::Source* source = source_info.mutable_source();
  source->set_source_id(row.getString("source_id"));
  source->set_user_id(row.getInt("user_id"));
  CHECK(source->mutable_schema()->ParseFromString(
      row.getString("source_schema")))
      << "Could not parse the source schema for source_id="
      << b64encode(row.getString("source_id"));
  source->set_name(row.getString("name"));
  source->set_private_(row.getBoolean("private"));
  source->set_frozen(row.getBoolean("frozen"));
  source->set_created(row.getString("created"));

  source_info.set_email(row.getString("email"));
  source_info.set_username(row.getString("username"));
}
} // anonymous namespace

std::error_condition MetadataSqlStore::get_source_by_id(
    SourceInfo& source_info, const std::string& source_id) {
  static constexpr char SOURCE_BY_ID_TEMPLATE[] =
      "SELECT source_id, user_id, source_schema, name, "
      "private, frozen, created, username, email "
      "FROM list_sources WHERE source_id = ?";
  if (!ensure_connected()) {
    return make_error_condition(SqlErr::SQL_CONNECTION_ERROR);
  }
  try {
    std::unique_ptr<sql::PreparedStatement> get_query{
      conn_->prepareStatement(SOURCE_BY_ID_TEMPLATE)};
    get_query->setString(1, source_id);
    std::unique_ptr<sql::ResultSet> row{get_query->executeQuery()};
    if (row->rowsCount() == 0) {
      return make_error_condition(SqlErr::INVALID_SOURCE_ID);
    }
    CHECK_EQ(row->rowsCount(), 1)
        << "A source's id is a primary key, cannot get more than 1 result";
    CHECK(row->next());
    list_sources_row_to_proto(source_info, *row);
    return make_error_condition(SqlErr::OK);
  } catch (const sql::SQLException& e) {
    LOG(WARNING) << "MetadataSqlStore::get_source_by_id() - sql exception - "
                 << e.what();
  }
  return make_error_condition(SqlErr::SQL_CONNECTION_ERROR);
}

std::error_condition MetadataSqlStore::get_sources_by(
    std::vector<SourceInfo>& sources_info, const int user_id,
    const std::string& username) {
  static constexpr char SOURCE_BY_TEMPLATE[] =
      "SELECT source_id, user_id, source_schema, name, "
      "private, frozen, created, username, email "
      "FROM list_sources ";
  static constexpr char BY_USER_ID[] =
      "WHERE user_id = ? ORDER BY created DESC";
  static constexpr char BY_USERNAME[] =
      "WHERE username = ? ORDER BY created DESC";

  CHECK((user_id == 0) != (username == ""))
      << "Specify either user_id or username";
  if (!ensure_connected()) {
    return make_error_condition(SqlErr::SQL_CONNECTION_ERROR);
  }
  try {
    std::unique_ptr<sql::PreparedStatement> get_query;
    std::unique_ptr<sql::ResultSet> row;
    string query{SOURCE_BY_TEMPLATE};
    if (user_id != 0) {
      query += BY_USER_ID;
      get_query.reset(conn_->prepareStatement(query));
      get_query->setInt(1, user_id);
      row.reset(get_query->executeQuery());
    } else {
      query += BY_USERNAME;
      get_query.reset(conn_->prepareStatement(query));
      get_query->setString(1, username);
      row.reset(get_query->executeQuery());
    }
    sources_info.clear();
    while (row->next()) {
      sources_info.emplace_back();
      auto& source_info = sources_info.back();
      list_sources_row_to_proto(source_info, *row);
    }
    return make_error_condition(SqlErr::OK);
  } catch (const sql::SQLException& e) {
    LOG(WARNING) << "MetadataSqlStore::get_source_by - sql exception - "
                 << "(user_id=" << user_id << "; username=" << username
                 << "): " << e.what();
  }
  return make_error_condition(SqlErr::SQL_CONNECTION_ERROR);
}

std::error_condition MetadataSqlStore::get_sources_by_user(
    vector<SourceInfo>& sources_info, const int user_id) {
  return get_sources_by(sources_info, user_id);
}

std::error_condition MetadataSqlStore::get_sources_by_username(
    std::vector<SourceInfo>& sources_info, const std::string& username) {
  return get_sources_by(sources_info, 0, username);
}

MetadataSqlStoreTasklet::MetadataSqlStoreTasklet(
    zmq::context_t& context, const std::string& host, const uint16_t port,
    const std::string& user, const std::string& pass, const std::string& db)
    : store_{new MetadataSqlStore{host, port, user, pass, db, true}},
      tasklet_{context, []() { }, [&]() { store_.reset(nullptr); }} {
}

std::error_condition MetadataSqlStoreTasklet::fetch_schema(
    core::Schema& schema, const std::string& source_id) {
  using namespace std::placeholders;
  if (!fetch_schema_.get()) {
    fetch_schema_.reset(new lib::Connection<fetch_schema_type>{
        std::move(tasklet_.connect(fetch_schema_type(std::bind(
            &MetadataSqlStore::fetch_schema, store_.get(), _1, _2))))});
  }
  return fetch_schema_->operator()(schema, source_id);
}

std::error_condition MetadataSqlStoreTasklet::create_source(
    const std::string& source_id, int user_id,
    const std::string& schema, const std::string& name,
    bool private_, bool frozen) {
  using namespace std::placeholders;
  if (!create_source_.get()) {
    create_source_.reset(new lib::Connection<create_source_type>{
        std::move(tasklet_.connect(create_source_type(std::bind(
            &MetadataSqlStore::create_source, store_.get(),
            _1, _2, _3, _4, _5, _6))))
            });
  }
  return create_source_->operator()(
      source_id, user_id, schema, name, private_, frozen);
}

std::error_condition MetadataSqlStoreTasklet::delete_source(
    int& rows_updated, const std::string& source_id) {
  using namespace std::placeholders;
  if (!delete_source_.get()) {
    delete_source_.reset(new lib::Connection<delete_source_type>{
        std::move(tasklet_.connect(delete_source_type(std::bind(
            &MetadataSqlStore::delete_source, store_.get(), _1, _2))))});
  }
  return delete_source_->operator()(rows_updated, source_id);
}

std::error_condition MetadataSqlStoreTasklet::get_source_by_id(
    SourceInfo& source_info, const std::string& source_id) {
  using namespace std::placeholders;
  if (!get_source_by_id_.get()) {
    get_source_by_id_.reset(new lib::Connection<get_source_by_id_type>{
        std::move(tasklet_.connect(get_source_by_id_type(std::bind(
            &MetadataSqlStore::get_source_by_id, store_.get(), _1, _2))))});
  }
  return get_source_by_id_->operator()(source_info, source_id);
}

std::error_condition MetadataSqlStoreTasklet::get_sources_by_user(
    std::vector<SourceInfo>& sources_info, const int user_id) {
  using namespace std::placeholders;
  if (!get_sources_by_user_.get()) {
    get_sources_by_user_.reset(new lib::Connection<get_sources_by_user_type>{
        std::move(tasklet_.connect(get_sources_by_user_type(std::bind(
            &MetadataSqlStore::get_sources_by_user, store_.get(), _1, _2))))});
  }
  return get_sources_by_user_->operator()(sources_info, user_id);
}

std::error_condition MetadataSqlStoreTasklet::get_sources_by_username(
    std::vector<SourceInfo>& sources_info, const std::string& username) {
  using namespace std::placeholders;
  if (!get_sources_by_username_.get()) {
    get_sources_by_username_.reset(
        new lib::Connection<get_sources_by_username_type>{std::move(
            tasklet_.connect(get_sources_by_username_type(std::bind(
                    &MetadataSqlStore::get_sources_by_username, store_.get(), _1, _2))))
              });
  }
  return get_sources_by_username_->operator()(sources_info, username);
}


}}}  // namespace reinferio::saltfish::store
