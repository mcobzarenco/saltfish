#ifndef REINFERIO_SALTFISH_RECORD_SUMMARIZER_HPP
#define REINFERIO_SALTFISH_RECORD_SUMMARIZER_HPP

#include "service_utils.hpp"
#include "reinferio/saltfish.pb.h"
#include "treadmill/moments_summarizer.hpp"
#include "treadmill/categorical_histogram_summarizer.hpp"

#include <riakpp/client.hpp>

#include <string>
#include <vector>


namespace Json {
class Value;
}

namespace reinferio { namespace saltfish {

using treadmill::RealVariableSummarizer;
using treadmill::CategoricalVariableSummarizer;

struct RealSummarizerFactory {
  typedef std::unique_ptr<RealVariableSummarizer> pointer;

  virtual ~RealSummarizerFactory() {}
  virtual pointer operator()() const = 0;

  template<class T>
  static const RealSummarizerFactory& defaultFor() {
    struct DefaultFactory : RealSummarizerFactory {
      pointer operator()() const override {
        return pointer{new T{}};
      }
    };
    static DefaultFactory defaultFactory;
    return defaultFactory;
  }

};

struct CategoricalSummarizerFactory {
  typedef std::unique_ptr<CategoricalVariableSummarizer> pointer;

  virtual ~CategoricalSummarizerFactory() {}
  virtual pointer operator()() const = 0;

  template<class T>
  static const CategoricalSummarizerFactory& defaultFor() {
    struct DefaultFactory : CategoricalSummarizerFactory {
      pointer operator()() const override {
        return pointer{new T{}};
      }
    };
    static DefaultFactory defaultFactory;
    return defaultFactory;
  }
};

struct RecordSummarizer {
  RecordSummarizer(
      const core::Schema& schema,
      const RealSummarizerFactory& real_factory,
      const CategoricalSummarizerFactory& categorical_factory);

  MaybeError push_record(const core::Record& record);
  const core::Schema& schema() const { return schema_; }

  std::string to_json() const;
 private:
  core::Schema schema_;

  std::vector<std::unique_ptr<RealVariableSummarizer>> real_summ_;
  std::vector<std::unique_ptr<CategoricalVariableSummarizer>> categorical_summ_;
};

struct SummarizerMap {
  SummarizerMap(riak::client& riak_client, std::string schemas_bucket)
      : riak_client_{riak_client}, schemas_bucket_{std::move(schemas_bucket)} {
  }

  void push_request(RequestType type, const std::string& msg);
  std::string to_json(const std::string& source_id);
 private:
  core::Schema fetch_schema(const std::string& source_id);

  riak::client& riak_client_;
  const std::string schemas_bucket_;
  std::unordered_map<std::string, RecordSummarizer> summarizers_;
};

}}  // namespace reinferio::saltfish

#endif  // REINFERIO_SALTFISH_RECORD_SUMMARIZER_HPP
