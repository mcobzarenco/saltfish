#include "service_utils.hpp"

#include "service.pb.h"
#include "source.pb.h"

#include <google/protobuf/descriptor.h>
#include <glog/logging.h>

#include <set>


namespace reinferio {
namespace saltfish {

using namespace std;

PutRecordsReplier::PutRecordsReplier(
    const vector<string>& record_ids, rpcz::reply<PutRecordsResponse> reply)
    : record_ids_(record_ids), n_records_(record_ids.size()),
      ok_received_(0), reply_(reply), already_replied_(false) {}

PutRecordsReplier::~PutRecordsReplier() {
  // LOG(INFO) << "Destroying a PutRecordsReplier with " << n_records_;
}

void PutRecordsReplier::reply(PutRecordsResponse::Status status,
                              const string& msg) {
  lock_guard<mutex> reply_lock(reply_mutex_);
  if (already_replied_)
    return;

  if(status == PutRecordsResponse::OK) {
    ok_received_++;
    CHECK_LE(ok_received_, n_records_)
        << "Received more responses than expected";
    if (ok_received_ == n_records_) {
      PutRecordsResponse response;
      response.set_status(PutRecordsResponse::OK);
      for (const auto& rid : record_ids_) {
        response.add_record_ids(rid);
      }
      // LOG(INFO) << response.DebugString();
      reply_.send(response);
      already_replied_ = true;
    }
  } else {
    PutRecordsResponse response;
    response.set_status(status);
    response.set_msg(msg);
    LOG(INFO) << response.DebugString();
    reply_.send(response);
    already_replied_ = true;
    return;
  }
}


}  // namespace saltfish
}  // namespace reinferio
