#ifndef REINFERIO_SALTFISH_SERVICE_UTILS_HPP
#define REINFERIO_SALTFISH_SERVICE_UTILS_HPP

#include "service.rpcz.h"

#include <rpcz/rpcz.hpp>

#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>


namespace reinferio {
namespace saltfish {

bool schema_has_duplicates(const source::Schema& schema);


template<typename RecordIter>
std::pair<bool, std::string> put_records_check_schema(
    const source::Schema& schema, RecordIter begin, RecordIter end) {
  int exp_reals{0}, exp_cats{0};
  for (auto feature : schema.features()) {
    if (feature.feature_type() == source::Feature::INVALID) {
      std::ostringstream msg;
      msg << "Source unusable as its schema contains a feature marked as invalid "
          << "(feature_name=" << feature.name() << ")";
      return std::make_pair(false, msg.str());
    } else if (feature.feature_type() == source::Feature::REAL) {
      exp_reals++;
    } else if (feature.feature_type() == source::Feature::CATEGORICAL) {
      exp_cats++;
    } else {
      return std::make_pair(false, "Source contains a feature unsupported by saltfish");
    }
  }
  int index{0};
  for (auto record = begin; record != end; ++record) {
    if (record->reals_size() != exp_reals) {
      std::ostringstream msg;
      msg << "Record with index " << index << " contains " << record->reals_size()
          << " real features (expected "<< exp_reals << ")";
      return std::make_pair(false, msg.str());
    } else if (record->cats_size() != exp_cats) {
      std::ostringstream msg;
      msg << "Record with index " << index << " contains " << record->cats_size()
          << " categorical features (expected "<< exp_cats << ")";
      return std::make_pair(false, msg.str());
    }
    index++;
  }
  return std::make_pair(true, "");
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

}  // namespace saltfish
}  // namespace reinferio

#endif  // REINFERIO_SALTFISH_SERVICE_UTILS_HPP
