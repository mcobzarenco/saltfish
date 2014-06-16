#include "exact_quantile_summarizer.hpp"

#include <json/value.h>
#include <boost/range/algorithm/copy.hpp>


namespace reinferio { namespace treadmill {

void ExactQuantileSummarizer::pushValue(double new_value) noexcept {
  pushValueFast(new_value);
}

void ExactQuantileSummarizer::updateJsonSummary(Json::Value& summary)
    const noexcept {
  auto& quantiles = summary["quantiles"];
  int i = 0;
  for (auto quantile : quantiles_at_splits(5)) quantiles[i++] = quantile;
}

}}  // namespace reinferio::treadmill
