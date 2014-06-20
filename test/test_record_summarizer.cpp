#include "record_summarizer.hpp"
#include "treadmill/moments_summarizer.hpp"
#include "treadmill/categorical_histogram_summarizer.hpp"
#include "reinferio/core.pb.h"

#include <cereal/archives/binary.hpp>
#include <cereal/archives/json.hpp>
#include <gtest/gtest.h>

#include <sstream>
#include <vector>

namespace reinferio { namespace saltfish { namespace {

TEST(RecordSummarizerTest, Serialization) {
  const std::vector<
    std::pair<const char *, core::Feature::FeatureType>> schema_spec = {
    {"x", core::Feature::REAL},
    {"y", core::Feature::REAL},
    {"z", core::Feature::REAL},
    {"cat", core::Feature::CATEGORICAL},
  };

  core::Schema schema;
  for (const auto& feat : schema_spec) {
    core::Feature* feature{schema.add_features()};
    feature->set_name(feat.first);
    feature->set_feature_type(feat.second);
  }

  StandardSummarizer record_summ{schema};
  std::stringstream bin_stream, text_stream;
  {
    cereal::JSONOutputArchive text_archive(text_stream);
    cereal::BinaryOutputArchive bin_archive(bin_stream);
    text_archive(record_summ);
    bin_archive(record_summ);
  }
  cereal::JSONInputArchive in_text(text_stream);
  cereal::BinaryInputArchive in_binary(bin_stream);
  StandardSummarizer bin_record_summ;
  StandardSummarizer text_record_summ;
  in_binary(bin_record_summ);
  in_text(text_record_summ);

  std::string schema_str, text_schema_str, bin_schema_str;
  schema.SerializeToString(&schema_str);
  bin_record_summ.schema().SerializeToString(&bin_schema_str);
  text_record_summ.schema().SerializeToString(&text_schema_str);
  EXPECT_EQ(schema_str, bin_schema_str);
  EXPECT_EQ(schema_str, text_schema_str);

  EXPECT_EQ(record_summ.to_json(), bin_record_summ.to_json());
  EXPECT_EQ(record_summ.to_json(), text_record_summ.to_json());
}

}}}  // namespace reinfeio::treadmill::<anonymous>
