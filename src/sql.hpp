#ifndef REINFERIO_SALTFISH_SQL_POOL_HPP
#define REINFERIO_SALTFISH_SQL_POOL_HPP

#include "tasklet.hpp"
#include "reinferio/core.pb.h"

#include <cppconn/connection.h>
#include <boost/optional.hpp>
#include <boost/thread/tss.hpp>
#include <zmq.hpp>

#include <memory>
#include <mutex>
#include <string>


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
  virtual boost::optional<std::vector<std::string>> fetch_schema(
      const std::string& source_id) = 0;

  virtual boost::optional<int> create_source(
      const std::string& source_id, int user_id,
      const std::string& schema, const std::string& name) = 0;

  virtual boost::optional<int> delete_source(const std::string& source_id) = 0;

  virtual boost::optional<std::vector<core::Source>> list_sources(
      const int user_id) = 0;
};

class MetadataSqlStore : MetadataStore {
 public:
  MetadataSqlStore(
      const std::string& host, const uint16_t port, const std::string& user,
      const std::string& pass, const std::string& db,
      const bool thread_init_end = true);
  ~MetadataSqlStore() { close(); }

  virtual boost::optional<std::vector<std::string>> fetch_schema(
      const std::string& source_id) override;
  virtual boost::optional<int> create_source(
      const std::string& source_id, int user_id,
      const std::string& schema, const std::string& name) override;
  virtual boost::optional<int> delete_source(
      const std::string& source_id) override;
  virtual boost::optional<std::vector<core::Source>> list_sources(
      const int user_id) override;

  bool ensure_connected();
  void close();
 private:
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

  boost::optional<std::vector<std::string>> fetch_schema(
      const std::string& source_id);
  boost::optional<int> create_source(
      const std::string& source_id, int user_id,
      const std::string& schema, const std::string& name);
  boost::optional<int> delete_source(const std::string& source_id);
  boost::optional<std::vector<core::Source>> list_sources(
      const int user_id);

  using fetch_schema_type = std::function<
    boost::optional<std::vector<std::string>>(const std::string&)>;
  using create_source_type = std::function<boost::optional<int>(
      const std::string&, int, const std::string&, const std::string&)>;
  using delete_source_type =
      std::function<boost::optional<int>(const std::string&)>;
  using list_sources_type =
      std::function<boost::optional<std::vector<core::Source>>(const int)>;

 private:
  template<typename T>
  using ts_ptr = boost::thread_specific_ptr<T>;

  std::unique_ptr<MetadataSqlStore> store_;
  lib::Tasklet tasklet_;
  ts_ptr<lib::Connection<fetch_schema_type>> fetch_schema_;
  ts_ptr<lib::Connection<create_source_type>> create_source_;
  ts_ptr<lib::Connection<delete_source_type>> delete_source_;
  ts_ptr<lib::Connection<list_sources_type>> list_sources_;
};

}}}  // namespace reinferio::saltfish::store

#endif  // REINFERIO_SALTFISH_SQL_POOL_HPP
