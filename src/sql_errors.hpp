#ifndef REINFERIO_SALTFISH_SQL_ERRORS_HPP
#define REINFERIO_SALTFISH_SQL_ERRORS_HPP

#include <system_error>
#include <string>

namespace {

}

namespace reinferio { namespace saltfish { namespace store {

enum class SqlErr {
  OK = 0,
  INVALID_DATASET_ID,
  INVALID_USER_ID,
  INVALID_USERNAME,
  DUPLICATE_DATASET_NAME,
  SQL_CONNECTION_ERROR,
  UNKNOWN_ERROR,
  NUM_ERRORS
};

class SqlErrCategory : public std::error_category {
 public:
  SqlErrCategory() noexcept {}
  virtual const char* name() const noexcept override { return "SqlError"; }
  virtual std::string message(int error) const noexcept override {
    return message(static_cast<SqlErr>(error));
  }
  const char* message(SqlErr error) const noexcept {
    switch(error) {
      case SqlErr::OK:
        return "OK";
      case SqlErr::INVALID_DATASET_ID:
        return "No dataset exists with the provided id.";
      case SqlErr::INVALID_USER_ID:
        return "No user exists with the provided id.";
      case SqlErr::INVALID_USERNAME:
        return "No user exists with the provided username.";
      case SqlErr::DUPLICATE_DATASET_NAME:
        return "A dataset with the same name already exists.";
      case SqlErr::SQL_CONNECTION_ERROR:
        return "Could not connect to MariaDB.";
      default:
        return "Unknown error";
    }
  }
};

const SqlErrCategory& sql_error_category();

inline std::error_condition make_error_condition(SqlErr error) {
  return std::error_condition(static_cast<int>(error), sql_error_category());
}

}}}  // namespace reinferio::saltfish::store

namespace std {
template<> class is_error_condition_enum<reinferio::saltfish::store::SqlErr>
    : public true_type {};
}

#endif  // REINFERIO_SALTFISH_SQL_ERRORS_HPP
