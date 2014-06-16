#include "moments_summarizer.hpp"

#include <json/value.h>


namespace reinferio { namespace treadmill {

void MomentsSummarizer::pushValue(double new_value) noexcept {
  pushValueFast(new_value);
}

void MomentsSummarizer::updateJsonSummary(Json::Value& summary)
    const noexcept {
  summary["mean"] = mean();
  summary["variance"] = variance();
  summary["num_values"] = Json::UInt64{num_values()};
  summary["num_missing"] = Json::UInt64{num_missing()};
}

}}  // namespace reinferio::treadmill
