#ifndef REINFERIO_SALTFISH_SERVICE_UTILS_HPP
#define REINFERIO_SALTFISH_SERVICE_UTILS_HPP

#include "reinferio/saltfish.rpcz.h"

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
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <utility>


namespace reinferio { namespace saltfish {

inline std::string gen_random_string(const uint32_t width=16) {
  static const size_t BLOCK_SIZE = sizeof(uint64_t);
  static thread_local std::mt19937_64 gen(
      std::chrono::high_resolution_clock::now().time_since_epoch().count() +
      static_cast<const uint64_t>(reinterpret_cast<const size_t>(&width)));
  CHECK_EQ(0, width % BLOCK_SIZE)
      << "width needs to be a multiple of " << BLOCK_SIZE;
  uint64_t uniform{0};
  std::string id;
  id.reserve(width);
  for (uint32_t i = 0; i < width; i += BLOCK_SIZE) {
    uniform = gen();
    id.append(reinterpret_cast<const char*>(&uniform),
              BLOCK_SIZE / sizeof(const char));
  }
  return id;
}

inline int64_t gen_random_int64() {
  std::string id = gen_random_string(sizeof(uint64_t));
  return *reinterpret_cast<const int64_t*>(id.data());
}

inline std::string string_to_hex(const std::string& source_id) {
  constexpr char hex[]{"0123456789abcdef"};
  std::stringstream out;
  for (const unsigned char& c : source_id)  out << hex[c >> 4] << hex[c & 0x0F];
  return out.str();
}

inline bool schema_has_duplicates(const core::Schema& schema) {
    using Compare = bool(*)(const std::string*, const std::string*);
    std::set<const std::string*, Compare> unique_names{
        [](const std::string* a, const std::string* b) { return *a < *b; } };
    for(const auto& feature : schema.features()) {
        unique_names.insert(&feature.name());
    }
    return unique_names.size() != static_cast<size_t>(schema.features().size());
}

inline bool schema_has_invalid_features(const core::Schema& schema) {
  auto is_invalid = [](const core::Feature& feat) -> bool {
    return feat.feature_type() == core::Feature::INVALID; };
  using Compare = bool(*)(const std::string*, const std::string*);
  return std::any_of(schema.features().begin(),
                     schema.features().end(), is_invalid);
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
    const core::Schema& schema, const core::Record& record) {
  int exp_reals{0}, exp_cats{0};
  for (auto feature : schema.features()) {
    if (feature.feature_type() == core::Feature::INVALID) {
      std::ostringstream msg;
      msg << "Source unusable as its schema contains a feature marked as invalid"
          << " (feature_name=" << feature.name() << ")";
      return MaybeError{msg.str()};
    } else if (feature.feature_type() == core::Feature::REAL) {
      exp_reals++;
    } else if (feature.feature_type() == core::Feature::CATEGORICAL) {
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
  if (already_replied_) return;
  already_replied_ = true;
  error_handler();
}

}}  // namespace reinferio::saltfish

#endif  // REINFERIO_SALTFISH_SERVICE_UTILS_HPP
