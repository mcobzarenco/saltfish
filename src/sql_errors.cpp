#include "sql_errors.hpp"


namespace reinferio { namespace saltfish { namespace store {

const SqlErrCategory& sql_error_category() {
  static const SqlErrCategory category;
  return category;
}

}}}  // namespace reinferio::saltfish::store
