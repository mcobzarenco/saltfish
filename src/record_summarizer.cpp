#include "record_summarizer.hpp"

#include <json/value.h>
#include <json/writer.h>

#include <condition_variable>
#include <memory>


namespace reinferio { namespace saltfish {

RecordSummarizer::RecordSummarizer(
    const core::Schema& schema,
    const RealSummarizerFactory& real_factory,
    const CategoricalSummarizerFactory& categorical_factory)
    : schema_{schema} {
  for (auto feature : schema_.features()) {
    if (feature.feature_type() == core::Feature::REAL) {
      real_summ_.emplace_back(real_factory());
    } else if (feature.feature_type() == core::Feature::CATEGORICAL) {
      categorical_summ_.emplace_back(categorical_factory());
    }
  }
}

MaybeError RecordSummarizer::push_record(const core::Record& record) {
  MaybeError err{check_record(schema_, record)};
  if (!err) {
    CHECK_EQ(real_summ_.size(), record.reals().size());
    for (size_t r = 0; r < real_summ_.size(); ++r) {
      real_summ_.at(r)->pushValue(record.reals(r));
    }
    CHECK_EQ(categorical_summ_.size(), record.cats().size());
    for (size_t c = 0; c < categorical_summ_.size(); ++c) {
      categorical_summ_.at(c)->pushValue(record.cats(c));
    }
  }
  return err;
}

std::string RecordSummarizer::to_json() const {
  Json::Value summary;
  size_t i_categorical = 0;
  size_t i_real = 0;

  for (auto feature : schema_.features()) {
    auto& new_value = summary[summary.size()];
    if (feature.feature_type() == core::Feature::REAL) {
      real_summ_[i_real++]->updateJsonSummary(new_value);
    } else if (feature.feature_type() == core::Feature::CATEGORICAL) {
      categorical_summ_[i_categorical++]->updateJsonSummary(new_value);
    }
  }
  auto writer = Json::FastWriter{};
  return writer.write(summary);
}


void SummarizerMap::push_request(RequestType type, const std::string& msg) {
  CHECK_EQ(type, PUT_RECORDS)
      << "SummarizerMap should only be subscribed to put_records()";
  PutRecordsRequest request;
  request.ParseFromString(msg);

  auto summarizer_it = summarizers_.find(request.source_id());
  if (summarizer_it == summarizers_.end()) {
    LOG(INFO) << "Schema not found for id="
              << string_to_hex(request.source_id())
              << "; retreiving it from Riak";
    auto schema = fetch_schema(request.source_id());
    auto map_pair = summarizers_.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(request.source_id()),
        std::forward_as_tuple(
            schema,
            RealSummarizerFactory::defaultFor<
            treadmill::MomentsSummarizer>(),
            CategoricalSummarizerFactory::defaultFor<
            treadmill::CategoricalHistogramSummarizer>())
                                         );
    CHECK(map_pair.second);
    summarizer_it = map_pair.first;
  } else {
    LOG(INFO) << "Source schema id=" << string_to_hex(request.source_id())
              << " already in the map.";
  }
  for (auto tag_record : request.records()) {
    summarizer_it->second.push_record(tag_record.record());
    // LOG(INFO) << tag_record.DebugString();
  }
  // LOG(INFO) << "State after req=" << summarizer_it->second.to_json();
}

std::string SummarizerMap::to_json(const std::string& source_id) {
  LOG(INFO) << "to_json " << string_to_hex(source_id);
  auto summarizer_it = summarizers_.find(source_id);
  if (summarizer_it == summarizers_.end()) {
    LOG(INFO) << "not found (" << summarizers_.size() << " in map)";
    // for (auto& p : summarizers_) {
    //   LOG(INFO) << string_to_hex(p.first) << " " << p.second.schema().features_size();
    // }
    return "not found";
  }
  return summarizer_it->second.to_json();
}

core::Schema SummarizerMap::fetch_schema(const std::string& source_id) {
  std::mutex fetch_mutex;
  std::condition_variable fetch_cv;
  std::unique_lock<std::mutex> lock(fetch_mutex);
  bool riak_replied{false};

  core::Schema schema;
  riak_client_.fetch(
      schemas_bucket_, source_id,
      [&] (riak::object obj, std::error_code err) {
        CHECK(obj.valid());
        if (!err) {
          schema.ParseFromString(obj.value());
        } else {
          LOG(FATAL) << "Could not retrieve schema";
        }
        riak_replied = true;
        fetch_cv.notify_one();
      });
  fetch_cv.wait(lock, [&] { return riak_replied; });
  return schema;
}

}}  // namespace reinferio::saltfish
