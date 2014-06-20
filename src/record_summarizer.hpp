#ifndef REINFERIO_SALTFISH_RECORD_SUMMARIZER_HPP
#define REINFERIO_SALTFISH_RECORD_SUMMARIZER_HPP

#include "service_utils.hpp"
#include "reinferio/saltfish.pb.h"
#include "treadmill/moments_summarizer.hpp"
#include "treadmill/categorical_histogram_summarizer.hpp"

#include <cereal/types/string.hpp>
#include <cereal/types/vector.hpp>

#include <json/value.h>
#include <json/writer.h>
#include <riakpp/client.hpp>

#include <string>
#include <vector>


namespace reinferio { namespace saltfish {

constexpr const char* SUMMARIZERS_BUCKET{"/summarizers"};

using treadmill::RealVariableSummarizer;
using treadmill::CategoricalVariableSummarizer;

template<typename RealSummarizer, typename CategoricalSummarizer>
struct RecordSummarizer {
  RecordSummarizer() = default;
  RecordSummarizer(const core::Schema& schema);

  RecordSummarizer& operator=(const RecordSummarizer& other);

  MaybeError push_record(const core::Record& record);
  const core::Schema& schema() const { return schema_; }

  std::string to_json() const;

  template<class Archive>
  inline void save(Archive& archive) const;
  template <class Archive>
  inline void load(Archive & archive);
 private:
  core::Schema schema_;

  std::vector<RealSummarizer> real_summ_;
  std::vector<CategoricalSummarizer> categorical_summ_;
};

template<typename RealSummarizer, typename CategoricalSummarizer>
RecordSummarizer<RealSummarizer, CategoricalSummarizer>::RecordSummarizer(
    const core::Schema& schema)
    : schema_{schema} {
  size_t n_reals{0}, n_categoricals{0};
  for (auto feature : schema_.features()) {
    if (feature.feature_type() == core::Feature::REAL) {
      ++n_reals;
    } else if (feature.feature_type() == core::Feature::CATEGORICAL) {
      ++n_categoricals;
    }
  }
  real_summ_.resize(n_reals);
  categorical_summ_.resize(n_categoricals);
}

template<typename RealSummarizer, typename CategoricalSummarizer>
RecordSummarizer<RealSummarizer, CategoricalSummarizer>&
RecordSummarizer<RealSummarizer, CategoricalSummarizer>::operator=(
    const RecordSummarizer& other) {
  schema_ = other.schema_;
  size_t n_reals{0}, n_categoricals{0};
  for (auto feature : schema_.features()) {
    if (feature.feature_type() == core::Feature::REAL) {
      ++n_reals;
    } else if (feature.feature_type() == core::Feature::CATEGORICAL) {
      ++n_categoricals;
    }
  }
  real_summ_.resize(n_reals);
  categorical_summ_.resize(n_categoricals);
  return *this;
}

template<typename RealSummarizer, typename CategoricalSummarizer>
MaybeError RecordSummarizer<RealSummarizer, CategoricalSummarizer>::push_record(
    const core::Record& record) {
  MaybeError err{check_record(schema_, record)};
  if (!err) {
    CHECK_EQ(real_summ_.size(), record.reals().size());
    for (size_t r = 0; r < real_summ_.size(); ++r) {
      real_summ_[r].pushValue(record.reals(r));
    }
    CHECK_EQ(categorical_summ_.size(), record.cats().size());
    for (size_t c = 0; c < categorical_summ_.size(); ++c) {
      categorical_summ_[c].pushValue(record.cats(c));
    }
  }
  return err;
}

template<typename RealSummarizer, typename CategoricalSummarizer>
std::string RecordSummarizer<RealSummarizer, CategoricalSummarizer>::to_json()
    const {
  Json::Value summary;
  size_t i_categorical = 0;
  size_t i_real = 0;

  for (auto feature : schema_.features()) {
    auto& new_value = summary[summary.size()];
    if (feature.feature_type() == core::Feature::REAL) {
      real_summ_[i_real++].updateJsonSummary(new_value);
    } else if (feature.feature_type() == core::Feature::CATEGORICAL) {
      categorical_summ_[i_categorical++].updateJsonSummary(new_value);
    }
  }
  auto writer = Json::FastWriter{};
  return writer.write(summary);
}

template<typename RealSummarizer, typename CategoricalSummarizer>
template<class Archive>
void RecordSummarizer<RealSummarizer, CategoricalSummarizer>::save(
    Archive& archive) const {
  archive(schema_.SerializeAsString(), real_summ_, categorical_summ_);
}

template<typename RealSummarizer, typename CategoricalSummarizer>
template <class Archive>
void RecordSummarizer<RealSummarizer, CategoricalSummarizer>::load(
    Archive & archive) {
  std::string schema_str;
  archive(schema_str);
  archive(real_summ_, categorical_summ_);
  schema_.ParseFromString(schema_str);
}

using StandardSummarizer = RecordSummarizer<
  treadmill::MomentsSummarizer, treadmill::CategoricalHistogramSummarizer>;

struct SummarizerMap {
  SummarizerMap(riak::client& riak_client, std::string schemas_bucket)
      : riak_client_{riak_client}, schemas_bucket_{std::move(schemas_bucket)},
        summarizers_bucket_{SUMMARIZERS_BUCKET} {
  }

  void push_request(RequestType type, const std::string& msg);
  std::string to_json(const std::string& source_id);
 private:
  bool fetch_schema(core::Schema& schema, const std::string& source_id);
  bool load_summarizer(
      StandardSummarizer& summarizer, const std::string& source_id);
  bool save_summarizer(
      const std::string& source_id, const StandardSummarizer& summarizer);

  riak::client& riak_client_;
  const std::string schemas_bucket_;
  const std::string summarizers_bucket_;
  std::unordered_map<std::string, StandardSummarizer> summarizers_;
};

}}  // namespace reinferio::saltfish

#endif  // REINFERIO_SALTFISH_RECORD_SUMMARIZER_HPP
