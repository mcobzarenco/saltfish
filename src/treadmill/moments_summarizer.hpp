#ifndef REINFERIO_TREADMILL_MOMENTS_SUMMARIZER_HPP_
#define REINFERIO_TREADMILL_MOMENTS_SUMMARIZER_HPP_

#include "summarizer.hpp"

#include <cereal/cereal.hpp>

#include <cmath>
#include <limits>
#include <vector>

namespace reinferio { namespace treadmill {

class MomentsSummarizer : public RealVariableSummarizer {
 public:
  MomentsSummarizer() = default;

  void pushValue(double new_value) noexcept override;
  void updateJsonSummary(Json::Value& summary) const noexcept override;

  inline void pushValueFast(double new_value) noexcept;

  inline double mean() const noexcept;
  inline double variance() const noexcept;

  uint64_t num_values() const noexcept { return num_values_; }
  uint64_t num_missing() const noexcept { return num_missing_; }

  template<class Archive>
  void serialize(Archive & archive) {
    archive(mean_, m2_, num_values_, num_missing_);
  }

  inline bool operator ==(const MomentsSummarizer& o) {
    return (this->mean_ == o.mean_)
        && (this->m2_ == o.m2_)
        && (this->num_values_ == o.num_values_)
        && (this->num_missing_ == o.num_missing_);
  }

 private:
  double mean_ = 0.0;
  double m2_ = 0.0;
  double num_values_ = 0.0;
  uint64_t num_missing_ = 0;
};

// Inline Method Implementations
// =============================================================================

double MomentsSummarizer::mean() const noexcept {
  if (num_values_ == 0.0)
    return std::numeric_limits<double>::quiet_NaN();

  return mean_;
}

double MomentsSummarizer::variance() const noexcept {
  if (num_values_ == 0.0)
    return std::numeric_limits<double>::quiet_NaN();

  return m2_ / (num_values_ - 1.0);
}

void MomentsSummarizer::pushValueFast(double new_value) noexcept {
  if (std::isnan(new_value)) {
    ++num_missing_;
  } else {
    ++num_values_;
    double delta = new_value - mean_;
    mean_ += delta / num_values_;
    m2_ += delta * (new_value - mean_);
  }
}

}}  // namespace reinferio::treadmill

#endif  // #ifndef REINFERIO_TREADMILL_MOMENTS_SUMMARIZER_HPP_
