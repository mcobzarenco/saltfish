#ifndef REINFERIO_TREADMILL_EXACT_QUANTILE_SUMMARIZER_HPP_
#define REINFERIO_TREADMILL_EXACT_QUANTILE_SUMMARIZER_HPP_

#include "summarizer.hpp"

#include <glog/logging.h>
#include <boost/range/algorithm/sort.hpp>
#include <cmath>
#include <limits>
#include <vector>


namespace reinferio { namespace treadmill {

class ExactQuantileSummarizer : public RealVariableSummarizer {
 public:
  ExactQuantileSummarizer() = default;

  void pushValue(double new_value) noexcept override;
  void updateJsonSummary(Json::Value& summary) const noexcept override;

  inline void pushValueFast(double new_value) noexcept;

  inline double min() const noexcept;
  inline double max() const noexcept;
  inline double quantile_at(double phi) const noexcept;
  inline std::vector<double> quantiles_at_splits(uint32_t num_splits)
      const noexcept;

 private:
  inline double quantile_at_impl(double phi) const noexcept;

  mutable std::vector<double> values_;
  mutable bool sorted_ = true;
  double min_ = std::numeric_limits<double>::infinity();
  double max_ = -std::numeric_limits<double>::infinity();
};

// Inline Method Implementations
// =============================================================================

inline double ExactQuantileSummarizer::min() const noexcept {
  if (values_.empty())
    return std::numeric_limits<double>::quiet_NaN();

  return min_;
}

inline double ExactQuantileSummarizer::max() const noexcept {
  if (values_.empty())
    return std::numeric_limits<double>::quiet_NaN();

  return max_;
}

double ExactQuantileSummarizer::quantile_at(double phi) const noexcept {
  if (phi < 0.0) {
    LOG(WARNING) << "phi (" << phi << ") < 0";
    return -std::numeric_limits<double>::infinity();
  } else if (phi > 1.0) {
    LOG(WARNING) << "phi (" << phi << ") > 1";
    return std::numeric_limits<double>::infinity();
  } else if (values_.empty()) {
    return std::numeric_limits<double>::quiet_NaN();
  }

  if (!sorted_) {
    boost::sort(values_);
    sorted_ = true;
  }

  return quantile_at_impl(phi);
}

inline double ExactQuantileSummarizer::quantile_at_impl(double phi)
    const noexcept {
  // Based on R implementation of quantile (method 5) at:
  //   https://github.com/SurajGupta/r-source/blob/master/src/library/stats/R/quantile.R?source=cc
  static constexpr auto fuzz = 4.0 * std::numeric_limits<double>::epsilon();

  auto n = values_.size();
  auto index = phi * n + .5;
  auto floor_index = floor(index + fuzz);
  auto frac_index = index - floor_index;

  auto int_index = static_cast<size_t>(floor_index);
  if (int_index == 0) {
    return min_;
  } else {
    --int_index;
  }

  if (frac_index <= fuzz && frac_index >= -fuzz) {
    return values_[int_index];
  }

  DCHECK_LT(int_index + 1, values_.size()) << frac_index;
  return values_[int_index] * (1.0 - frac_index) +
      values_[int_index + 1] * frac_index;
}

std::vector<double> ExactQuantileSummarizer::quantiles_at_splits(
    uint32_t num_splits) const noexcept {
  if (values_.size() == 0) {
    return std::vector<double>(num_splits,
                               std::numeric_limits<double>::quiet_NaN());
  } else if (values_.size() == 1) {
    return std::vector<double>(num_splits, values_[0]);
  } else if (num_splits == 0) {
    return {};
  } else if (num_splits == 1) {
    return {min_};
  }

  if (!sorted_) {
    boost::sort(values_);
    sorted_ = true;
  }

  auto quantiles = std::vector<double>{};
  auto step = 1.0 / static_cast<double>(num_splits - 1);
  quantiles.reserve(num_splits);
  quantiles.push_back(min_);

  auto n_quantiles = num_splits - 2;
  auto phi = step;
  while (n_quantiles--) {
    quantiles.push_back(quantile_at_impl(phi));
    phi += step;
  }

  quantiles.push_back(max_);
  DCHECK_EQ(quantiles.size(), num_splits) << "with num_splits = " << num_splits;

  return quantiles;
}


void ExactQuantileSummarizer::pushValueFast(double new_value) noexcept {
  if (std::isnan(new_value))
    return;

  values_.push_back(new_value);
  if (new_value < min_) min_ = new_value;
  if (new_value > max_) {
    max_ = new_value;
  } else {
    sorted_ = false;
  }
}

}}  // namespace reinferio::treadmill

#endif  // #ifndef REINFERIO_TREADMILL_EXACT_QUANTILE_SUMMARIZER_HPP_
