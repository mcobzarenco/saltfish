#include "service_utils.hpp"
#include "reinferio/core.pb.h"
#include "reinferio/saltfish.pb.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <algorithm>
#include <iostream>
#include <memory>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>


using namespace std;
using namespace reinferio;

TEST(GenerateRandomString, CorrectFormat) {
  constexpr uint32_t N = 100;
  constexpr char hex[] = "0123456789abcdef";
  const unordered_set<char> hex_chars{hex, hex + sizeof(hex)};
  unordered_set<char> chars;
  string id, hex_id;
  for (uint32_t width = 8; width < N + 10; width+=8) {
    id = saltfish::gen_random_string(width);
    hex_id = saltfish::string_to_hex(id);
    chars.insert(id.begin(), id.end());
    EXPECT_EQ(width, id.size()) << hex_id;
    EXPECT_EQ(2 * width, hex_id.size()) << hex_id;
    EXPECT_TRUE(all_of(hex_id.begin(), hex_id.end(), [&hex_chars](const char c) {
          return hex_chars.count(c) != 0;
        })) << hex_id;
  }

  string test_id{" abcdefghijklmnop"};
  EXPECT_EQ("206162636465666768696a6b6c6d6e6f70",
            saltfish::string_to_hex(test_id));
}

TEST(GenerateRandomString, GeneratesUniqueStrings) {
  const uint32_t N = 1000000;
  unordered_set<string> ids;
  for (uint32_t n = 0; n < N; ++n)  ids.insert(saltfish::gen_random_string());
  EXPECT_EQ(N, ids.size());
}

TEST(SchemaHasDuplicatesTest, EmptyNoDupsAndDups) {
  core::Schema schema;
  EXPECT_FALSE(saltfish::schema_has_duplicates(schema))
      << "empty schema - does not have duplicates";

  core::Feature* feat{nullptr};
  feat = schema.add_features();
  feat->set_name("feature_1");
  feat->set_feature_type(core::Feature::REAL);
  EXPECT_FALSE(saltfish::schema_has_duplicates(schema))
          << "there are no duplicates";

  feat = schema.add_features();
  feat->set_name("feature_2");
  feat->set_feature_type(core::Feature::REAL);
  EXPECT_FALSE(saltfish::schema_has_duplicates(schema))
          << "there are no duplicates";

  feat = schema.add_features();
  feat->set_name("feature_3");
  feat->set_feature_type(core::Feature::CATEGORICAL);
  EXPECT_FALSE(saltfish::schema_has_duplicates(schema))
      << "there are no duplicates";

  // Adding a duplicate feature now
  feat = schema.add_features();
  feat->set_name("feature_1");
  feat->set_feature_type(core::Feature::CATEGORICAL);
  EXPECT_TRUE(saltfish::schema_has_duplicates(schema))
      << "feature_1 is duplicated";
}

class CheckRecordTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    core::Feature* feat{nullptr};
    feat = schema_.add_features();
    feat->set_name("real_1");
    feat->set_feature_type(core::Feature::REAL);

    feat = schema_.add_features();
    feat->set_name("real_2");
    feat->set_feature_type(core::Feature::REAL);

    feat = schema_.add_features();
    feat->set_name("categorical_3");
    feat->set_feature_type(core::Feature::CATEGORICAL);
  }

  core::Schema schema_;
};

TEST_F(CheckRecordTest, Valid) {
  saltfish::PutRecordsRequest req;
  core::Record *record{nullptr};
  record = req.add_records()->mutable_record();
  record->add_reals(0.1234);
  record->add_reals(-852.32);
  record->add_cats("blue");
  EXPECT_FALSE(saltfish::check_record(schema_, *record));

  record = req.add_records()->mutable_record();
  record->add_reals(0.434);
  record->add_reals(-1052.32);
  record->add_cats("red");
  EXPECT_FALSE(saltfish::check_record(schema_, *record));

  for (auto& tagged : req.records()) {
      auto status = saltfish::check_record(schema_, tagged.record());
      EXPECT_FALSE(status);
      EXPECT_TRUE(status.what().empty());
  }
}

TEST_F(CheckRecordTest, MissingFeature) {
  core::Record record;
  record.add_reals(0.434);
  record.add_cats("red");
  auto status = saltfish::check_record(schema_, record);
  EXPECT_TRUE(static_cast<bool>(status));
  EXPECT_FALSE(status.what().empty());
}

TEST_F(CheckRecordTest, TooManyFeatures) {
  core::Record record;
  record.add_reals(0.434);
  record.add_reals(-1052.32);
  record.add_cats("red");
  record.add_cats("yellow");
  auto status = saltfish::check_record(schema_, record);
  EXPECT_TRUE(static_cast<bool>(status));
  EXPECT_FALSE(status.what().empty());
}

TEST_F(CheckRecordTest, IncorrectFeatureType) {
  core::Record record;
  record.add_reals(0.434);
  record.add_cats("red");
  record.add_cats("yellow");
  auto status = saltfish::check_record(schema_, record);
  EXPECT_TRUE(static_cast<bool>(status));
  EXPECT_FALSE(status.what().empty());
}

TEST_F(CheckRecordTest, InvalidFeatureInSchema) {
  core::Schema invalid_schema = schema_;
  core::Feature* feat = invalid_schema.add_features();
  feat->set_name("problematic_feature");
  feat->set_feature_type(core::Feature::INVALID);

  core::Record record;
  auto status = saltfish::check_record(invalid_schema, record);
  EXPECT_TRUE(static_cast<bool>(status));
  EXPECT_THAT(status.what(), testing::HasSubstr("invalid"));
  EXPECT_THAT(status.what(), testing::HasSubstr("problematic_feature"));
}

TEST(ReplySyncTest, ReplyWithSuccess) {
  constexpr uint32_t N_THREADS{10};
  int n_calls{0};
  auto handler = [&]() { ++n_calls; };
  saltfish::ReplySync replier{N_THREADS, handler};

  std::vector<std::thread> threads;
  for (uint32_t i = 0; i < N_THREADS; ++i) {
    EXPECT_EQ(0, n_calls);
    threads.emplace_back(std::thread([&replier]() {
          volatile int unused = 0;
          for (auto x = 0; x < 1000000; ++x) { ++unused; }
          replier.ok();
        }));
  }
  for (auto& th : threads) {
    th.join();
  }
  EXPECT_EQ(N_THREADS, replier.ok_received());
  EXPECT_EQ(1, n_calls);
}

TEST(ReplySyncTest, ReplyWithError) {
  constexpr uint32_t N_THREADS{10};
  int n_calls_success{0};
  auto handler_success = [&]() { ++n_calls_success; };
  saltfish::ReplySync replier{N_THREADS, handler_success};

  std::vector<std::thread> threads;
  int n_calls_error{0};
  auto handler_error = [&]() { ++n_calls_error; };
  auto report_error = [&replier, &handler_error]() {
    volatile int unused = 0;
    for (auto x = 0; x < 1000000; ++x) { ++unused; }
    replier.error(handler_error);
  };
  auto report_success = [&replier]() {
    volatile int unused = 0;
    for (auto x = 0; x < 1000000; ++x) { ++unused; }
    replier.ok();
  };

  threads.emplace_back(std::thread(report_error));
  threads.emplace_back(std::thread(report_error));
  for (uint32_t i = 0; i < N_THREADS - 2; ++i) {
    threads.emplace_back(std::thread{report_success});
  }
  for (auto& th : threads) { th.join(); }
  EXPECT_EQ(N_THREADS - 2, replier.ok_received());
  EXPECT_EQ(0, n_calls_success);
  EXPECT_EQ(1, n_calls_error);
}

/*                                    main                                    */

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  testing::FLAGS_gtest_color = "yes";
  return RUN_ALL_TESTS();
}
