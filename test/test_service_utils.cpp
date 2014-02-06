#include "service_utils.hpp"
#include "source.pb.h"
#include "service.pb.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <iostream>
#include <memory>
#include <thread>
#include <utility>
#include <vector>


using namespace std;
using namespace reinferio;

TEST(IsValidUuidBytesTest, IsValidUuidBytes) {
  string u1;
  EXPECT_FALSE(saltfish::is_valid_uuid_bytes(u1));

  string u2{"\x00\x01\x02\x03\x04\x05\x06", 7};
  EXPECT_FALSE(saltfish::is_valid_uuid_bytes(u2));

  string u3{"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f", 16};
  EXPECT_TRUE(saltfish::is_valid_uuid_bytes(u3));
}

TEST(SchemaHasDuplicatesTest, EmptyNoDupsAndDups) {
  source::Schema schema;
  EXPECT_FALSE(saltfish::schema_has_duplicates(schema))
      << "empty schema - does not have duplicates";

  source::Feature* feat{nullptr};
  feat = schema.add_features();
  feat->set_name("feature_1");
  feat->set_feature_type(source::Feature::REAL);
  EXPECT_FALSE(saltfish::schema_has_duplicates(schema))
          << "there are no duplicates";

  feat = schema.add_features();
  feat->set_name("feature_2");
  feat->set_feature_type(source::Feature::REAL);
  EXPECT_FALSE(saltfish::schema_has_duplicates(schema))
          << "there are no duplicates";

  feat = schema.add_features();
  feat->set_name("feature_3");
  feat->set_feature_type(source::Feature::CATEGORICAL);
  EXPECT_FALSE(saltfish::schema_has_duplicates(schema))
      << "there are no duplicates";

  // Adding a duplicate feature now
  feat = schema.add_features();
  feat->set_name("feature_1");
  feat->set_feature_type(source::Feature::CATEGORICAL);
  EXPECT_TRUE(saltfish::schema_has_duplicates(schema))
      << "feature_1 is duplicated";
}

class CheckRecordTest : public ::testing::Test {
 protected:
  virtual void SetUp() {
    source::Feature* feat{nullptr};
    feat = schema_.add_features();
    feat->set_name("real_1");
    feat->set_feature_type(source::Feature::REAL);

    feat = schema_.add_features();
    feat->set_name("real_2");
    feat->set_feature_type(source::Feature::REAL);

    feat = schema_.add_features();
    feat->set_name("categorical_3");
    feat->set_feature_type(source::Feature::CATEGORICAL);
  }

  source::Schema schema_;
};

TEST_F(CheckRecordTest, Valid) {
  saltfish::PutRecordsRequest req;
  source::Record *record{nullptr};
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
  source::Record record;
  record.add_reals(0.434);
  record.add_cats("red");
  auto status = saltfish::check_record(schema_, record);
  EXPECT_TRUE(static_cast<bool>(status));
  EXPECT_FALSE(status.what().empty());
}

TEST_F(CheckRecordTest, TooManyFeatures) {
  source::Record record;
  record.add_reals(0.434);
  record.add_reals(-1052.32);
  record.add_cats("red");
  record.add_cats("yellow");
  auto status = saltfish::check_record(schema_, record);
  EXPECT_TRUE(static_cast<bool>(status));
  EXPECT_FALSE(status.what().empty());
}

TEST_F(CheckRecordTest, IncorrectFeatureType) {
  source::Record record;
  record.add_reals(0.434);
  record.add_cats("red");
  record.add_cats("yellow");
  auto status = saltfish::check_record(schema_, record);
  EXPECT_TRUE(static_cast<bool>(status));
  EXPECT_FALSE(status.what().empty());
}

TEST_F(CheckRecordTest, InvalidFeatureInSchema) {
  source::Schema invalid_schema = schema_;
  source::Feature* feat = invalid_schema.add_features();
  feat->set_name("problematic_feature");
  feat->set_feature_type(source::Feature::INVALID);

  source::Record record;
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
