#include "categorical_histogram_summarizer.hpp"

#include <cereal/archives/binary.hpp>
#include <cereal/archives/json.hpp>
#include <gtest/gtest.h>
#include <json/json.h>

#include <sstream>
#include <vector>
#include <iostream>


namespace reinferio { namespace treadmill {
namespace {

using namespace std;

void validateJson(const CategoricalHistogramSummarizer& summarizer) {
  auto summary = Json::Value{};
  summarizer.updateJsonSummary(summary);
  EXPECT_EQ(summarizer.num_values(),
            summary["num_values"].asUInt64());
  EXPECT_EQ(summarizer.num_unique_values(),
            summary["num_unique_values"].asUInt64());
  EXPECT_EQ(summarizer.num_missing(),
            summary["num_missing"].asUInt64());
  EXPECT_EQ(summarizer.num_unique_values(),
            summary["histogram"].size());

  auto sum_of_counts = uint64_t{0}, counted_unique = uint64_t{0};
  for (const auto& value_count : summarizer) {
    EXPECT_EQ(value_count.second,
              summary["histogram"][value_count.first].asUInt64())
        << "in counts for key '" << value_count.first << "'.";
    sum_of_counts += value_count.second;
    ++counted_unique;
  }

  EXPECT_EQ(counted_unique, summarizer.num_unique_values());
  EXPECT_EQ(sum_of_counts, summarizer.num_values());
}

TEST(CategoricalHistogramSummarizerTest, NoData) {
  auto summarizer = CategoricalHistogramSummarizer{};
  validateJson(summarizer);
}

TEST(CategoricalHistogramSummarizerTest, OnlyMissing) {
  auto summarizer = CategoricalHistogramSummarizer{};
  for (int i = 0; i < 100; ++i) summarizer.pushValueFast("");
  for (int i = 0; i < 100; ++i) summarizer.pushValue("");
  EXPECT_EQ(0, summarizer.num_unique_values());
  EXPECT_EQ(0, summarizer.num_values());
  EXPECT_EQ(200, summarizer.num_missing());
  validateJson(summarizer);
}

TEST(CategoricalHistogramSummarizerTest, SomeDataAndSomeMissing) {
  auto summarizer = CategoricalHistogramSummarizer{};
  const auto some_data = std::vector<std::string>{"a", "b", "a", "a", "", "b",
                                                  "b", "c", "a", "", "", "a"};
  const auto num_a = size_t{5}, num_b = size_t{3}, num_c = size_t{1};
  const auto num_missing = size_t{3};
  const auto num_iterations = 100;

  for (int i = 0; i < num_iterations; ++i) {
    for (const auto& value : some_data) {
      if (i % 3 == 0) {
        summarizer.pushValueFast(value);
      } else {
        summarizer.pushValue(value);
      }
    }
  }

  EXPECT_EQ(num_iterations * num_a, summarizer.value_count("a"));
  EXPECT_EQ(num_iterations * num_b, summarizer.value_count("b"));
  EXPECT_EQ(num_iterations * num_c, summarizer.value_count("c"));
  EXPECT_EQ(num_iterations * num_missing, summarizer.num_missing());
  EXPECT_EQ(num_iterations * (some_data.size() - num_missing),
            summarizer.num_values());

  validateJson(summarizer);
}

TEST(CategoricalHistogramSummarizerTest, Serialization) {
  CategoricalHistogramSummarizer summarizer;
  const auto some_data = std::vector<std::string>{
    "a", "b", "a", "a", "", "b", "b", "c", "a", "", "", "a"};
  const auto num_iterations = 100;
  int iter{0};

  do {
    stringstream bin_stream, text_stream;
    {
      cereal::JSONOutputArchive text_archive(text_stream);
      cereal::BinaryOutputArchive bin_archive(bin_stream);
      text_archive(summarizer);
      bin_archive(summarizer);
    }
    cereal::JSONInputArchive in_text(text_stream);
    cereal::BinaryInputArchive in_binary(bin_stream);
    CategoricalHistogramSummarizer bin_summ, text_summ;
    in_text(text_summ);
    in_binary(bin_summ);
    cerr << text_stream.str() << endl;

    ASSERT_TRUE(summarizer == bin_summ);
    ASSERT_TRUE(summarizer == text_summ);

    for (int i = 0; i < num_iterations; ++i) {
      for (const auto& value : some_data) {
        if (i % 3 == 0) {
          summarizer.pushValueFast(value);
        } else {
          summarizer.pushValue(value);
        }
      }
    }
  } while (iter++ < 1);
}

}}}  // namespace reinferio::treadmill
