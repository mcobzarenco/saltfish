#include "moments_summarizer.hpp"

#include <cereal/archives/binary.hpp>
#include <cereal/archives/json.hpp>
#include <gtest/gtest.h>
#include <json/value.h>

#include <cmath>

// Feels like C.
#define ARRAY_SIZEOF(x) (sizeof((x)) / sizeof((x)[0]))

// Gah! Google internal GTest provides this, they need to bring the open source
// release up to date.
#define EXPECT_DOUBLE_MAYBE_NAN_EQ(expected_x, actual_x) \
    do { \
      double expected = (expected_x); \
      double actual = (actual_x); \
      if (std::isnan(expected)) { \
        EXPECT_TRUE(std::isnan(actual)); \
      } else { \
        EXPECT_DOUBLE_EQ(expected, actual); \
      } \
    } while(false)

namespace reinferio { namespace treadmill {
namespace {

using namespace std;

// Checks JSON summary corresponds to values reported by getters.
void validateJson(const MomentsSummarizer& summarizer) {
  // Wrap in for-loop to make sure calling updateJsonSummary doesn't break it.
  for (int i = 0; i < 2; ++ i) {
    Json::Value summary;
    summarizer.updateJsonSummary(summary);

    EXPECT_TRUE(summary["mean"].isDouble());
    EXPECT_TRUE(summary["variance"].isDouble());
    EXPECT_TRUE(summary["num_values"].isUInt());
    EXPECT_TRUE(summary["num_missing"].isUInt());

    EXPECT_DOUBLE_MAYBE_NAN_EQ(
        summarizer.mean(), summary["mean"].asDouble());

    EXPECT_DOUBLE_MAYBE_NAN_EQ(
        summarizer.variance(), summary["variance"].asDouble());

    EXPECT_DOUBLE_MAYBE_NAN_EQ(
        summarizer.num_values(), summary["num_values"].asDouble());

    EXPECT_DOUBLE_MAYBE_NAN_EQ(
        summarizer.num_missing(), summary["num_missing"].asDouble());
  }
}

TEST(MomentsSummarizerTest, NoData) {
  MomentsSummarizer summarizer;
  EXPECT_TRUE(std::isnan(summarizer.mean()));
  EXPECT_TRUE(std::isnan(summarizer.variance()));
  EXPECT_EQ(0, summarizer.num_values());
  EXPECT_EQ(0, summarizer.num_missing());

  validateJson(summarizer);
}

TEST(MomentsSummarizerTest, OnePoint) {
  MomentsSummarizer summarizer;
  summarizer.pushValueFast(1.0);
  EXPECT_DOUBLE_EQ(1.0, summarizer.mean());
  EXPECT_TRUE(std::isnan(summarizer.variance()));
  EXPECT_EQ(1, summarizer.num_values());
  EXPECT_EQ(0, summarizer.num_missing());

  validateJson(summarizer);
}

TEST(MomentsSummarizerTest, Constant) {
  MomentsSummarizer summarizer;
  for (int i = 0; i < 100; ++i)
    summarizer.pushValueFast(0.5);

  EXPECT_DOUBLE_EQ(0.5, summarizer.mean());
  EXPECT_DOUBLE_EQ(0.0, summarizer.variance());
  EXPECT_EQ(100, summarizer.num_values());
  EXPECT_EQ(0, summarizer.num_missing());
  validateJson(summarizer);
}

// Generated and computed with Octave.
const double kUniformDataMean = 6.62393087478386;
const double kUniformDataVariance = 8.02951823262016;
const double kUniformData[] = {
  8.096616126649639255674629, 3.181649503540009860103055,
  9.844950794006999572616223, 1.198884445414944321939288,
  7.530346911663036379991354, 3.433352135060776078034905,
  9.778810516082939940929464, 5.548905004581840216815181,
  3.955122903425210445504945, 5.631680459681391859305677,
  3.252665502783643081130549, 9.808684493479979948915570,
  9.339073455782562760418841, 8.564609586406440655537153,
  8.648600125150153417052934, 8.465793285128649259263511,
  8.370178861222195010327596, 2.797632269355984746539434,
  9.648757574641646073132506, 5.382303541619091191705593};

TEST(MomentsSummarizerTest, UniformDataNoMissing) {
  MomentsSummarizer summarizer;
  for (auto point : kUniformData) {
    summarizer.pushValueFast(point);
  }

  EXPECT_DOUBLE_EQ(kUniformDataMean, summarizer.mean());
  EXPECT_DOUBLE_EQ(kUniformDataVariance, summarizer.variance());
  EXPECT_EQ(ARRAY_SIZEOF(kUniformData), summarizer.num_values());
  EXPECT_EQ(0, summarizer.num_missing());

  validateJson(summarizer);
}

TEST(MomentsSummarizerTest, UniformDataWithMissing) {
  MomentsSummarizer summarizer;
  int k = 0, missing = 0;
  for (auto point : kUniformData) {
    // Every three points add a missing value.
    if (k % 3 == 0) {
      ++missing;
      summarizer.pushValueFast(std::numeric_limits<double>::quiet_NaN());
    }
    ++k;

    summarizer.pushValueFast(point);
  }

  EXPECT_DOUBLE_EQ(kUniformDataMean, summarizer.mean());
  EXPECT_DOUBLE_EQ(kUniformDataVariance, summarizer.variance());
  EXPECT_EQ(ARRAY_SIZEOF(kUniformData), summarizer.num_values());
  EXPECT_EQ(missing, summarizer.num_missing());

  validateJson(summarizer);
}

TEST(MomentsSummarizerTest, Serialization) {
  MomentsSummarizer summarizer;
  int k = 0, missing = 0;
  int iter{0};
  do {
    stringstream stream, text_stream;
    {
      cereal::JSONOutputArchive text_archive(text_stream);
      cereal::BinaryOutputArchive bin_archive(stream);
      text_archive(summarizer);
      bin_archive(summarizer);
    }
    stringstream in_stream{stream.str()}, in_text_stream{text_stream.str()};
    cereal::JSONInputArchive in_text(in_text_stream);
    cereal::BinaryInputArchive in_binary(in_stream);
    MomentsSummarizer bin_summ, text_summ;
    in_text(text_summ);
    in_binary(bin_summ);

    ASSERT_TRUE(summarizer == bin_summ);
    ASSERT_TRUE(summarizer == text_summ);

    for (auto point : kUniformData) {
      // Every three points add a missing value.
      if (k % 3 == 0) {
        ++missing;
        summarizer.pushValueFast(std::numeric_limits<double>::quiet_NaN());
      }
      ++k;

      summarizer.pushValueFast(point);
    }
  } while (iter++ < 1);
}


}  // namespace
}  // namespace treadmill
}  // namespace reinferio
