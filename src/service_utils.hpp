#ifndef REINFERIO_SALTFISH_SERVICE_UTILS_HPP
#define REINFERIO_SALTFISH_SERVICE_UTILS_HPP

#include "service.rpcz.h"

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/optional.hpp>
#include <glog/logging.h>
#include <rpcz/rpcz.hpp>

#include <cppconn/exception.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>
#include <mysql_driver.h>
#include <mysql_connection.h>

#include <algorithm>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>


namespace reinferio {
namespace saltfish {

inline bool is_valid_uuid_bytes(const std::string& id) {
  return boost::uuids::uuid::static_size() == id.size();
}

inline boost::uuids::uuid from_string(const std::string& s) {
  CHECK(is_valid_uuid_bytes(s))
      << "a uuid has exactly " << boost::uuids::uuid::static_size() << " bytes";
  boost::uuids::uuid uuid;
  copy(s.begin(), s.end(), uuid.begin());
  return uuid;
}

inline std::string uuid_bytes_to_hex(const std::string& id) {
  return boost::uuids::to_string(from_string(id));
}

inline bool schema_has_duplicates(const source::Schema& schema){
    using Compare = bool(*)(const std::string*, const std::string*);
    std::set<const std::string*, Compare> unique_names{
        [](const std::string* a, const std::string* b) { return *a < *b; } };
    for(const auto& feature : schema.features()) {
        unique_names.insert(&feature.name());
    }
    return unique_names.size() != static_cast<size_t>(schema.features().size());
}

class MaybeError {
 public:
  MaybeError() = default;
  MaybeError(const MaybeError&) = default;
  MaybeError(MaybeError&&) = default;
  MaybeError(std::string msg)
      : err_{true}, msg_(std::move(msg)) {}

  explicit operator bool() const { return err_; }
  const std::string& what() const { return msg_; }
 private:
  bool err_ = false;
  std::string msg_;
};

inline MaybeError check_record(
    const source::Schema& schema, const source::Record& record) {
  int exp_reals{0}, exp_cats{0};
  for (auto feature : schema.features()) {
    if (feature.feature_type() == source::Feature::INVALID) {
      std::ostringstream msg;
      msg << "Source unusable as its schema contains a feature marked as invalid"
          << " (feature_name=" << feature.name() << ")";
      return MaybeError{msg.str()};
    } else if (feature.feature_type() == source::Feature::REAL) {
      exp_reals++;
    } else if (feature.feature_type() == source::Feature::CATEGORICAL) {
      exp_cats++;
    } else {
      return MaybeError{
        "Source schema contains a feature unsupported by saltfish"};
    }
  }
  if (record.reals_size() != exp_reals) {
    std::ostringstream msg;
    msg << "record contains " << record.reals_size()
        << " real features (expected "<< exp_reals << ")";
    return MaybeError{msg.str()};
  } else if (record.cats_size() != exp_cats) {
    std::ostringstream msg;
    msg << "record contains " << record.cats_size()
        << " categorical features (expected "<< exp_cats << ")";
    return MaybeError{msg.str()};
  }
  return MaybeError{};
}

class PutRecordsReplier {
 public:
  PutRecordsReplier(const std::vector<std::string>& record_ids,
                    rpcz::reply<PutRecordsResponse> reply);
  ~PutRecordsReplier();

  void reply(PutRecordsResponse::Status status, const std::string& msg);

 private:
  const std::vector<std::string> record_ids_;
  const std::size_t n_records_;

  std::uint32_t ok_received_;
  rpcz::reply<PutRecordsResponse> reply_;
  std::mutex reply_mutex_;
  bool already_replied_;
};

class ReplySync {
 public:
  using Postlude = std::function<void()>;

  ReplySync(uint32_t n_acks, Postlude success_handler)
      : n_acks_{n_acks}, success_{move(success_handler)} {}
  ~ReplySync() {}

  inline uint32_t ok_received() const { return ok_received_; }

  inline void ok();
  inline void error(Postlude error_handler);
 private:
  const uint32_t n_acks_;
  Postlude success_;

  std::mutex reply_mutex_;
  uint32_t ok_received_{0};
  bool already_replied_{false};
};

void ReplySync::ok() {
  std::lock_guard<std::mutex> reply_lock(reply_mutex_);
  ++ok_received_;
  CHECK_LE(ok_received_, n_acks_)
      << "Received more responses than expected";
  if (ok_received_ == n_acks_ && !already_replied_) {
    already_replied_ = true;
    success_();
  }
}

void ReplySync::error(Postlude error_handler) {
  std::lock_guard<std::mutex> reply_lock(reply_mutex_);
  already_replied_ = true;
  error_handler();
}


namespace sql {

inline boost::optional<std::string> fetch_source_schema(
    ::sql::Connection* conn, const std::string& source_id) {
  static constexpr char GET_SOURCE_TEMPLATE[] =
      "SELECT source_id, user_id, source_schema, name FROM sources "
      "WHERE source_id = ?";
  std::unique_ptr< ::sql::PreparedStatement > get_query{
    conn->prepareStatement(GET_SOURCE_TEMPLATE)};
  get_query->setString(1, source_id);
  std::unique_ptr< ::sql::ResultSet > res{get_query->executeQuery()};
  if (res->rowsCount() > 0) {
    CHECK(res->rowsCount() == 1)
        << "Integrity constraint violated, source_id is a primary key";
    VLOG(0) << "source_id already exists";
    res->next();
    return boost::optional<std::string>{res->getString("source_schema")};
  }
  return boost::optional<std::string>();
}

}  // namespace sql

}}  // namespace reinferio::saltfish

#endif  // REINFERIO_SALTFISH_SERVICE_UTILS_HPP
