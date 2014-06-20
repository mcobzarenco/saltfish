#include "categorical_histogram_summarizer.hpp"

#include <json/value.h>


namespace reinferio { namespace treadmill {

void CategoricalHistogramSummarizer::pushValue(const std::string& new_value)
    noexcept {
  pushValueFast(new_value);
}

void CategoricalHistogramSummarizer::updateJsonSummary(Json::Value& summary)
   const noexcept {
  summary["num_values"] = Json::UInt64{num_values()};
  summary["num_unique_values"] = Json::UInt64{num_unique_values()};
  summary["num_missing"] = Json::UInt64{num_missing()};
  auto& histogram = summary["histogram"];
  for (const auto& value_count_pair : value_counts_) {
    histogram[value_count_pair.first] = Json::UInt64{value_count_pair.second};
  }
}

}} // namespace reinferio::treadmill
