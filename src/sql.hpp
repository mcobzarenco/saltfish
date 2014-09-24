#ifndef REINFERIO_SALTFISH_SQL_POOL_HPP
#define REINFERIO_SALTFISH_SQL_POOL_HPP

#include "tasklet.hpp"
#include "reinferio/core.pb.h"
#include "reinferio/saltfish.pb.h"

#include <cppconn/connection.h>
#include <boost/thread/tss.hpp>
#include <zmq.hpp>

#include <memory>
#include <mutex>
#include <string>
#include <system_error>
#include <vector>


namespace sql {
class Driver;
class Connection;
}

namespace reinferio { namespace saltfish { namespace store {

std::unique_ptr<sql::Connection> connect_to_sql(
    const std::string& host, const std::string& user,
    const std::string& pass, const std::string& db);

class MetadataStore {
 public:
  virtual std::error_condition fetch_schema(
      core::Schema& schema, const std::string& dataset_id) = 0;
  virtual std::error_condition create_dataset(
      const std::string& dataset_id, int user_id,
      const std::string& schema, const std::string& name,
      bool private_, bool frozen) = 0;
  virtual std::error_condition delete_dataset(
      int& rows_updated, const std::string& dataset_id) = 0;

  virtual std::error_condition get_dataset_by_id(
      DatasetDetail& dataset_detail, const std::string& dataset_id) = 0;
  virtual std::error_condition get_datasets_by_user(
      std::vector<DatasetDetail>& dataset_details, const int user_id) = 0;
  virtual std::error_condition get_datasets_by_username(
      std::vector<DatasetDetail>& dataset_details, const std::string& username) = 0;
};

class MetadataSqlStore : MetadataStore {
 public:
  MetadataSqlStore(
      const std::string& host, const uint16_t port, const std::string& user,
      const std::string& pass, const std::string& db,
      const bool thread_init_end = true);
  ~MetadataSqlStore() { close(); }

  virtual std::error_condition fetch_schema(
      core::Schema& schema, const std::string& dataset_id) override;
  virtual std::error_condition create_dataset(
      const std::string& dataset_id, int user_id,
      const std::string& schema, const std::string& name,
      bool private_, bool frozen) override;
  virtual std::error_condition delete_dataset(
      int& rows_updated, const std::string& dataset_id) override;

  virtual std::error_condition get_dataset_by_id(
      DatasetDetail& dataset_detail, const std::string& dataset_id) override;
  virtual std::error_condition get_datasets_by_user(
      std::vector<DatasetDetail>& dataset_details, const int user_id) override;
  virtual std::error_condition get_datasets_by_username(
      std::vector<DatasetDetail>& dataset_details, const std::string& username) override;

  bool ensure_connected();
  void close();
 private:
  std::error_condition get_datasets_by(
      std::vector<DatasetDetail>& dataset_details, const int user_id=0,
      const std::string& username="");

  const std::string host_;
  const uint16_t port_;
  const std::string user_;
  const std::string pass_;
  const std::string db_;
  const bool thread_init_end_;

  sql::Driver* driver_;
  std::unique_ptr<sql::Connection> conn_;
};

class MetadataSqlStoreTasklet {
 public:
  MetadataSqlStoreTasklet(
      zmq::context_t& context, const std::string& host, const uint16_t port,
      const std::string& user, const std::string& pass, const std::string& db);

  std::error_condition fetch_schema(
      core::Schema& schema, const std::string& dataset_id);
  std::error_condition create_dataset(
      const std::string& dataset_id, int user_id,
      const std::string& schema, const std::string& name,
      bool private_, bool frozen);
  std::error_condition delete_dataset(
      int& rows_updated, const std::string& dataset_id);

  std::error_condition get_dataset_by_id(
      DatasetDetail& dataset_detail, const std::string& dataset_id);
  std::error_condition get_datasets_by_user(
      std::vector<DatasetDetail>& dataset_details, const int user_id);
  std::error_condition get_datasets_by_username(
      std::vector<DatasetDetail>& dataset_details, const std::string& username);

  using fetch_schema_type = std::function<
    std::error_condition(core::Schema&, const std::string&)>;
  using create_dataset_type = std::function<std::error_condition(
      const std::string&, int, const std::string&, const std::string&, bool, bool)>;
  using delete_dataset_type = std::function<
    std::error_condition(int&, const std::string&)>;

  using get_dataset_by_id_type = std::function<
    std::error_condition(DatasetDetail&, const std::string&)>;
  using get_datasets_by_user_type = std::function<std::error_condition(
      std::vector<DatasetDetail>& dataset_details, const int user_id)>;
  using get_datasets_by_username_type = std::function<std::error_condition(
      std::vector<DatasetDetail>& dataset_details, const std::string& username)>;

 private:
  template<typename T>
  using ts_ptr = boost::thread_specific_ptr<T>;

  std::unique_ptr<MetadataSqlStore> store_;
  lib::Tasklet tasklet_;
  ts_ptr<lib::Connection<fetch_schema_type>> fetch_schema_;
  ts_ptr<lib::Connection<create_dataset_type>> create_dataset_;
  ts_ptr<lib::Connection<delete_dataset_type>> delete_dataset_;

  ts_ptr<lib::Connection<get_dataset_by_id_type>> get_dataset_by_id_;
  ts_ptr<lib::Connection<get_datasets_by_user_type>> get_datasets_by_user_;
  ts_ptr<lib::Connection<get_datasets_by_username_type>> get_datasets_by_username_;
};

}}}  // namespace reinferio::saltfish::store

#endif  // REINFERIO_SALTFISH_SQL_POOL_HPP
