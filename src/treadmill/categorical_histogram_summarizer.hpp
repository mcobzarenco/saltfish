#ifndef REINFERIO_TREADMILL_CATEGORICAL_HISTOGRAM_SUMMARIZER_HPP_
#define REINFERIO_TREADMILL_CATEGORICAL_HISTOGRAM_SUMMARIZER_HPP_

#include <cstdint>
#include <unordered_map>

#include "summarizer.hpp"


namespace reinferio { namespace treadmill {

class CategoricalHistogramSummarizer : public CategoricalVariableSummarizer {
 private:
  typedef std::unordered_map<std::string, uint64_t> ValueCountMap;

 public:
  typedef ValueCountMap::value_type value_type;
  typedef ValueCountMap::const_iterator iterator;
  typedef iterator const_iterator;

  CategoricalHistogramSummarizer() = default;

  void pushValue(const std::string& new_value) noexcept override;
  void updateJsonSummary(Json::Value& summary) const noexcept override;

  inline void pushValueFast(const std::string& new_value) noexcept;
  inline uint64_t value_count(const std::string& value);

  uint64_t num_values_with_duplicates() const noexcept { return num_values_; }
  uint64_t num_missing() const noexcept { return num_missing_; }
  uint64_t num_unique_values() const noexcept { return value_counts_.size(); }

  const_iterator begin() const { return value_counts_.begin(); }
  const_iterator end() const { return value_counts_.end(); }

 private:
  uint64_t num_values_ = 0;
  uint64_t num_missing_ = 0;
  ValueCountMap value_counts_;
};

// Inline Method Implementations
// =============================================================================

void CategoricalHistogramSummarizer::pushValueFast(const std::string& new_value)
    noexcept {
  if (new_value.empty()) {
    ++num_missing_;
  } else {
    ++num_values_;
    ++value_counts_.emplace(new_value, uint64_t(0)).first->second;
  }
}

uint64_t CategoricalHistogramSummarizer::value_count(const std::string& value) {
  auto it = value_counts_.find(value);
  if (it == value_counts_.end()) return 0;
  return it->second;
}

}}  // namespace reinferio::treadmill

#endif  // REINFERIO_TREADMILL_CATEGORICAL_HISTOGRAM_SUMMARIZER_HPP_
