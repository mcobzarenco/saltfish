#ifndef REINFERIO_TREADMILL_SUMMARIZER_HPP_
#define REINFERIO_TREADMILL_SUMMARIZER_HPP_

#include <boost/property_tree/ptree_fwd.hpp>
#include <json/forwards.h>
#include <string>


namespace reinferio { namespace treadmill {

class Summarizer {
 public:
  // Updates a summary property tree with the values computed by this
  // summarizer. The summary maps statistic names to their values.
  virtual void updateJsonSummary(Json::Value& summary)
      const noexcept = 0;
};

class RealVariableSummarizer : public Summarizer {
 public:
  // Updates the summary with a new value.
  virtual void pushValue(double new_value) noexcept = 0;
};

class CategoricalVariableSummarizer : public Summarizer {
 public:
  // Updates the summary with a new value. Slow only in that it's a virtual
  // call.
  virtual void pushValue(const std::string& new_value) noexcept = 0;
};

}}  // namespace reinferio::treadmill

#endif  // #ifndef REINFERIO_TREADMILL_SUMMARIZER_HPP_
