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


namespace reinferio {
namespace saltfish {
namespace store {

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
    if (thread_init_end_) driver_->threadInit();
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
  if (driver_ != nullptr && thread_init_end_) driver_->threadEnd();
  LOG(INFO) << "Closing SQL connection";
}

std::error_condition MetadataSqlStore::fetch_schema(
    core::Schema& schema, const std::string& dataset_id) {
  static constexpr char GET_DATASET_TEMPLATE[] =
      "SELECT source_id, user_id, source_schema, name FROM sources "
      "WHERE source_id = ?";
  if (!ensure_connected()) {
    return make_error_condition(SqlErr::SQL_CONNECTION_ERROR);
  }
  try {
    std::unique_ptr<sql::PreparedStatement> get_query{
      conn_->prepareStatement(GET_DATASET_TEMPLATE)};
    get_query->setString(1, dataset_id);
    std::unique_ptr<sql::ResultSet> res{get_query->executeQuery()};
    if (res->rowsCount() > 0) {
      CHECK(res->rowsCount() == 1)
          << "Integrity constraint violated, dataset_id is a primary key";
      res->next();
      CHECK(schema.ParseFromString(res->getString("source_schema")))
          << "Could not parse the dataset schema for dataset_id="
          << b64encode(dataset_id);
      return make_error_condition(SqlErr::OK);
    }
    return make_error_condition(SqlErr::INVALID_DATASET_ID);
  } catch (const sql::SQLException& e) {
    LOG(WARNING) << "MetadataSqlStore::fetch_schema() - sql exception - "
                 << e.what();
  }
  return make_error_condition(SqlErr::SQL_CONNECTION_ERROR);
}

std::error_condition MetadataSqlStore::create_dataset(
    const std::string& dataset_id, int user_id,
    const std::string& schema, const std::string& name,
    bool private_, bool frozen) {
  static constexpr char CREATE_DATASET_TEMPLATE[] =
      "INSERT INTO sources (source_id, user_id, source_schema, name, "
      "private, frozen) VALUES (?, ?, ?, ?, ?, ?)";
  if (!ensure_connected()) {
    return make_error_condition(SqlErr::SQL_CONNECTION_ERROR);
  }
  try {
    unique_ptr<sql::PreparedStatement> query{
      conn_->prepareStatement(CREATE_DATASET_TEMPLATE)};
    query->setString(1, dataset_id);
    query->setInt(2, user_id);
    query->setString(3, schema);
    query->setString(4, name);
    query->setBoolean(5, private_);
    query->setBoolean(6, frozen);
    query->executeUpdate();
    return make_error_condition(SqlErr::OK);
  } catch (const sql::SQLException& e) {
    string msg{e.what()};
    LOG(WARNING) << "MetadataSqlStore::create_dataset() - sql exception - "
                 << msg;
    if (msg.find("sources_user_name") != string::npos) {
      return make_error_condition(SqlErr::DUPLICATE_DATASET_NAME);
    } else if (msg.find("FOREIGN KEY (`user_id`)") != string::npos) {
      return make_error_condition(SqlErr::INVALID_USER_ID);
    }
  }
  return make_error_condition(SqlErr::SQL_CONNECTION_ERROR);
}

std::error_condition MetadataSqlStore::delete_dataset(
    int& rows_updated, const std::string& dataset_id) {
  static constexpr char DELETE_DATASET_TEMPLATE[] =
      "DELETE FROM sources WHERE source_id = ?";
  if (!ensure_connected()) {
    return make_error_condition(SqlErr::SQL_CONNECTION_ERROR);
  }
  try {
    unique_ptr<sql::PreparedStatement> query{
      conn_->prepareStatement(DELETE_DATASET_TEMPLATE)};
    query->setString(1, dataset_id);
    rows_updated = query->executeUpdate();
    CHECK(rows_updated == 0 || rows_updated == 1)
        << "source_id is a primary key, a max of 1 row can be affected";
    return make_error_condition(SqlErr::OK);
  } catch (const sql::SQLException& e) {
    LOG(WARNING) << "MetadataSqlStore::delete_dataset() - sql exception - "
                 << e.what();
  }
  return make_error_condition(SqlErr::SQL_CONNECTION_ERROR);
}

namespace {
void list_datasets_row_to_proto(
    DatasetDetail& dataset_detail, sql::ResultSet& row) {
  core::Dataset* dataset = dataset_detail.mutable_dataset();
  dataset->set_id(row.getString("source_id"));
  dataset->set_user_id(row.getInt("user_id"));
  CHECK(dataset->mutable_schema()->ParseFromString(
      row.getString("source_schema")))
      << "Could not parse the dataset schema for dataset_id="
      << b64encode(row.getString("source_id"));
  dataset->set_name(row.getString("name"));
  dataset->set_private_(row.getBoolean("private"));
  dataset->set_frozen(row.getBoolean("frozen"));
  dataset->set_created(row.getString("created"));

  dataset_detail.set_email(row.getString("email"));
  dataset_detail.set_username(row.getString("username"));
}
} // anonymous namespace

std::error_condition MetadataSqlStore::get_dataset_by_id(
    DatasetDetail& dataset_detail, const std::string& dataset_id) {
  static constexpr char DATASET_BY_ID_TEMPLATE[] =
      "SELECT source_id, user_id, source_schema, name, "
      "private, frozen, created, username, email "
      "FROM list_sources WHERE source_id = ?";
  if (!ensure_connected()) {
    return make_error_condition(SqlErr::SQL_CONNECTION_ERROR);
  }
  try {
    std::unique_ptr<sql::PreparedStatement> get_query{
      conn_->prepareStatement(DATASET_BY_ID_TEMPLATE)};
    get_query->setString(1, dataset_id);
    std::unique_ptr<sql::ResultSet> row{get_query->executeQuery()};
    if (row->rowsCount() == 0) {
      return make_error_condition(SqlErr::INVALID_DATASET_ID);
    }
    CHECK_EQ(row->rowsCount(), 1)
        << "A dataset's id is a primary key, cannot get more than 1 result";
    CHECK(row->next());
    list_datasets_row_to_proto(dataset_detail, *row);
    return make_error_condition(SqlErr::OK);
  } catch (const sql::SQLException& e) {
    LOG(WARNING) << "MetadataSqlStore::get_dataset_by_id() - sql exception - "
                 << e.what();
  }
  return make_error_condition(SqlErr::SQL_CONNECTION_ERROR);
}

std::error_condition MetadataSqlStore::get_datasets_by(
    std::vector<DatasetDetail>& dataset_details, const int user_id,
    const std::string& username) {
  static constexpr char DATASET_BY_TEMPLATE[] =
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
    string query{DATASET_BY_TEMPLATE};
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
    dataset_details.clear();
    while (row->next()) {
      dataset_details.emplace_back();
      auto& dataset_detail = dataset_details.back();
      list_datasets_row_to_proto(dataset_detail, *row);
    }
    return make_error_condition(SqlErr::OK);
  } catch (const sql::SQLException& e) {
    LOG(WARNING) << "MetadataSqlStore::get_dataset_by - sql exception - "
                 << "(user_id=" << user_id << "; username=" << username
                 << "): " << e.what();
  }
  return make_error_condition(SqlErr::SQL_CONNECTION_ERROR);
}

std::error_condition MetadataSqlStore::get_datasets_by_user(
    vector<DatasetDetail>& dataset_details, const int user_id) {
  return get_datasets_by(dataset_details, user_id);
}

std::error_condition MetadataSqlStore::get_datasets_by_username(
    std::vector<DatasetDetail>& dataset_details, const std::string& username) {
  return get_datasets_by(dataset_details, 0, username);
}

MetadataSqlStoreTasklet::MetadataSqlStoreTasklet(
    zmq::context_t& context, const std::string& host, const uint16_t port,
    const std::string& user, const std::string& pass, const std::string& db)
    : store_{new MetadataSqlStore{host, port, user, pass, db, true}},
      tasklet_{context, []() { }, [&]() { store_.reset(nullptr); }} {
}

std::error_condition MetadataSqlStoreTasklet::fetch_schema(
    core::Schema& schema, const std::string& dataset_id) {
  using namespace std::placeholders;
  if (!fetch_schema_.get()) {
    fetch_schema_.reset(new lib::Connection<fetch_schema_type>{
        std::move(tasklet_.connect(fetch_schema_type(std::bind(
            &MetadataSqlStore::fetch_schema, store_.get(), _1, _2))))});
  }
  return fetch_schema_->operator()(schema, dataset_id);
}

std::error_condition MetadataSqlStoreTasklet::create_dataset(
    const std::string& dataset_id, int user_id,
    const std::string& schema, const std::string& name,
    bool private_, bool frozen) {
  using namespace std::placeholders;
  if (!create_dataset_.get()) {
    create_dataset_.reset(new lib::Connection<create_dataset_type>{
        std::move(tasklet_.connect(create_dataset_type(std::bind(
            &MetadataSqlStore::create_dataset, store_.get(),
            _1, _2, _3, _4, _5, _6))))
            });
  }
  return create_dataset_->operator()(
      dataset_id, user_id, schema, name, private_, frozen);
}

std::error_condition MetadataSqlStoreTasklet::delete_dataset(
    int& rows_updated, const std::string& dataset_id) {
  using namespace std::placeholders;
  if (!delete_dataset_.get()) {
    delete_dataset_.reset(new lib::Connection<delete_dataset_type>{
        std::move(tasklet_.connect(delete_dataset_type(std::bind(
            &MetadataSqlStore::delete_dataset, store_.get(), _1, _2))))});
  }
  return delete_dataset_->operator()(rows_updated, dataset_id);
}

std::error_condition MetadataSqlStoreTasklet::get_dataset_by_id(
    DatasetDetail& datasets_detail, const std::string& dataset_id) {
  using namespace std::placeholders;
  if (!get_dataset_by_id_.get()) {
    get_dataset_by_id_.reset(new lib::Connection<get_dataset_by_id_type>{
        std::move(tasklet_.connect(get_dataset_by_id_type(std::bind(
            &MetadataSqlStore::get_dataset_by_id, store_.get(), _1, _2))))});
  }
  return get_dataset_by_id_->operator()(datasets_detail, dataset_id);
}

std::error_condition MetadataSqlStoreTasklet::get_datasets_by_user(
    std::vector<DatasetDetail>& datasets_details, const int user_id) {
  using namespace std::placeholders;
  if (!get_datasets_by_user_.get()) {
    get_datasets_by_user_.reset(new lib::Connection<get_datasets_by_user_type>{
        std::move(tasklet_.connect(get_datasets_by_user_type(std::bind(
            &MetadataSqlStore::get_datasets_by_user, store_.get(), _1, _2))))});
  }
  return get_datasets_by_user_->operator()(datasets_details, user_id);
}

std::error_condition MetadataSqlStoreTasklet::get_datasets_by_username(
    std::vector<DatasetDetail>& datasets_details, const std::string& username) {
  using namespace std::placeholders;
  if (!get_datasets_by_username_.get()) {
    get_datasets_by_username_.reset(
        new lib::Connection<get_datasets_by_username_type>{std::move(
            tasklet_.connect(get_datasets_by_username_type(std::bind(
                    &MetadataSqlStore::get_datasets_by_username, store_.get(), _1, _2))))
              });
  }
  return get_datasets_by_username_->operator()(datasets_details, username);
}


        } // namespa
    }
}  // namespace reinferio::saltfish::store
