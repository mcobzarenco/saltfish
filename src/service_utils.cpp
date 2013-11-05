#include "service_utils.hpp"

#include "service.pb.h"
#include "source.pb.h"

#include <google/protobuf/descriptor.h>

#include <set>
#include <sstream>
#include <thread>

namespace reinferio {
namespace saltfish {

using namespace std;


string schema_to_str(const source::Schema& schema) {
  auto ft_desc = source::Feature::FeatureType_descriptor();
  ostringstream ss;
  bool first = true;
  ss << "[";
  for (source::Feature f : schema.features()) {
    if(!first)
      ss << ", ";
    else
      first = false;
    ss << "(\"" << f.name() << "\":"
       << ft_desc->FindValueByNumber(f.feature_type())->name() << ")";
  }
  ss << "]";
  return ss.str();
}

bool schema_has_duplicates(const source::Schema& schema) {
  // TODO: Maybe use a more generic function that this..
  set<string> names;
  for (const auto& feature : schema.features()) {
    if (names.count(feature.name()) == 1)
      return true;
    names.insert(feature.name());
  }
  return false;
}

pair<bool, string> put_records_check_schema(const source::Schema& schema,
                                            const PutRecordsRequest& request) {
  int exp_reals{0}, exp_cats{0};
  for (auto feature : schema.features()) {
    if (feature.feature_type() == source::Feature::INVALID) {
      ostringstream msg;
      msg << "Source unusable as its schema contains a feature marked as invalid "
          << "(feature_name=" << feature.name() << ")";
      return make_pair(false, msg.str());
    } else if (feature.feature_type() == source::Feature::REAL) {
      exp_reals++;
    } else if (feature.feature_type() == source::Feature::CATEGORICAL) {
      exp_cats++;
    } else {
      return make_pair(false, "Source contains a feature unsupported by saltfish");
    }
  }

  int index{0};
  for (auto record : request.records()) {
    if (record.reals_size() != exp_reals) {
      ostringstream msg;
      msg << "Record with index " << index << " contains " << record.reals_size()
          << " real features (expected "<< exp_reals << ")";
      return make_pair(false, msg.str());
    } else if (record.cats_size() != exp_cats) {
      ostringstream msg;
      msg << "Record with index " << index << " contains " << record.cats_size()
          << " categorical features (expected "<< exp_cats << ")";
      return make_pair(false, msg.str());
    }
    index++;
  }
  return make_pair(true, "");
}

PutRecordsReplier::PutRecordsReplier(
    const vector<string>& record_ids, rpcz::reply<PutRecordsResponse> reply)
    : record_ids_(record_ids), n_records_(record_ids.size()),
      n_resp_received_(0), reply_(reply), already_replied_(false) {}

PutRecordsReplier::~PutRecordsReplier() {
  // LOG(INFO) << "Destroying a PutRecordsReplier with " << n_records_;
}

// TODO(mcobzarenco): Change to single mutex.
void PutRecordsReplier::reply(PutRecordsResponse::Status status, const string& msg) {
  if (already_replied_) {
    return;
  }

  if (status != PutRecordsResponse::OK) {
    lock_guard<mutex> reply_lock(reply_mutex_);
    if (already_replied_)
      return;
    PutRecordsResponse response;
    response.set_status(status);
    response.set_msg(msg);
    reply_.send(response);
    already_replied_ = true;
    return;
  } else {
    lock_guard<mutex> n_resp_recieved_lock(n_resp_received_mutex_);
    n_resp_received_++;
  }

  CHECK_LE(n_resp_received_, n_records_) << "Received more responses than expected";
  if (n_resp_received_ == n_records_) {
    lock_guard<mutex> reply_lock(reply_mutex_);
    if (already_replied_)
      return;

    PutRecordsResponse response;
    response.set_status(PutRecordsResponse::OK);
    for (const auto& rid : record_ids_) {
      response.add_record_ids(rid);
    }
    reply_.send(response);
    already_replied_ = true;
  }
}


}  // namespace saltfish
}  // namespace reinferio
