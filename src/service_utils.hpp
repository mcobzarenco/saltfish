#ifndef REINFERIO_SALTFISH_SERVICE_UTILS_HPP
#define REINFERIO_SALTFISH_SERVICE_UTILS_HPP

#include "reinferio/saltfish.rpcz.h"

#include <boost/archive/iterators/transform_width.hpp>
#include <boost/iterator/transform_iterator.hpp>
#include <glog/logging.h>
#include <rpcz/rpcz.hpp>

#include <cppconn/exception.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>
#include <mysql_driver.h>
#include <mysql_connection.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <utility>


namespace reinferio { namespace saltfish {

inline std::string gen_random_string(const uint32_t width) {
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

inline uint64_t gen_random_uint64() {
  std::string id = gen_random_string(sizeof(uint64_t));
  return *reinterpret_cast<const uint64_t*>(id.data());
}

inline int64_t get_monotonous_ticks() {
  static std::atomic<int64_t> last_tick{0};

  int64_t timestamp{
    std::chrono::system_clock::now().time_since_epoch()
      / std::chrono::microseconds(1)};
  int64_t old_tick{last_tick.load()};
  int64_t new_tick{0};
  do {
    new_tick = std::max(timestamp, old_tick + 1);
  } while (!std::atomic_compare_exchange_weak(
             &last_tick, &old_tick, new_tick));
  return new_tick;
}

inline std::string b64encode(const std::string& binary) {
  struct from_6_bits {
    using result_type = char;
    char operator()(char byte) const {
      static constexpr const char* lookup =
          "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
      DCHECK_GE(byte, 0);
      DCHECK_LT(byte, 64);
      return lookup[static_cast<size_t>(byte)];
    }
  };
  using boost::transform_iterator;
  using boost::archive::iterators::transform_width;
  using Iterator = transform_iterator<
    from_6_bits, transform_width<std::string::const_iterator, 6, 8>>;

  std::string transformed;
  transformed.reserve(binary.size() * 4 / 3 + 4);
  std::copy(Iterator{binary.begin()}, Iterator{binary.end()},
            std::back_inserter(transformed));

  uint8_t padding_size = (3 - (binary.size() % 3)) % 3;
  for (size_t i_padding = 0; i_padding < padding_size; ++i_padding)
    transformed.push_back('=');

  return transformed;
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
    return feat.type() == core::Feature::INVALID; };
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
  int exp_numericals{0}, exp_categoricals{0}, exp_texts{0};
  for (auto feature : schema.features()) {
    if (feature.type() == core::Feature::INVALID) {
      std::ostringstream msg;
      msg << "Source unusable as its schema contains an invalid feature "
          << " (feature_name=" << feature.name() << ")";
      return MaybeError{msg.str()};
    } else if (feature.type() == core::Feature::NUMERICAL) {
      exp_numericals++;
    } else if (feature.type() == core::Feature::CATEGORICAL) {
      exp_categoricals++;
    } else if (feature.type() == core::Feature::TEXT) {
      exp_texts++;
    } else {
      return MaybeError{
        "Source schema contains a feature unsupported by saltfish"};
    }
  }
  if (record.numericals_size() != exp_numericals) {
    std::ostringstream msg;
    msg << "record contains " << record.categoricals_size()
        << " real features (expected "<< exp_numericals << ")";
    return MaybeError{msg.str()};
  } else if (record.categoricals_size() != exp_categoricals) {
    std::ostringstream msg;
    msg << "record contains " << record.categoricals_size()
        << " categorical features (expected "<< exp_categoricals << ")";
    return MaybeError{msg.str()};
  } else if (record.texts_size() != exp_texts) {
    std::ostringstream msg;
    msg << "record contains " << record.texts_size()
        << " text features (expected "<< exp_texts << ")";
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
