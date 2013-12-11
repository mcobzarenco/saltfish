#include "service_utils.hpp"
#include "source.pb.h"
#include "service.pb.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <utility>
#include <tuple>

#include <iostream>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

using namespace std;
using namespace reinferio;
using saltfish::put_records_check_schema;

TEST(CanCopyUUIDTest, CopyUUID) {
    boost::uuids::uuid u1 = boost::uuids::random_generator()();
    boost::uuids::uuid u2;

    source::Source source;
    source.mutable_source_id()->assign(u1.begin(), u1.end());
    std::copy(source.source_id().begin(), source.source_id().end(), u2.data);
    EXPECT_EQ (u1, u2);
    cout << to_string(u1) << " " << to_string(u2) << endl;
}

TEST(SchemaHasDuplicatesTest, EmptyNoDupsAndDups) {
  source::Schema schema;
  EXPECT_FALSE(saltfish::schema_has_duplicates(schema))
      << "empty schema - does not have duplicates";

  source::Feature* feat{nullptr};
  feat = schema.add_features();
  feat->set_name("feature_1");
  feat->set_feature_type(source::Feature::REAL);

  feat = schema.add_features();
  feat->set_name("feature_2");
  feat->set_feature_type(source::Feature::REAL);

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

class PutRecordsCheckSchemaTest : public ::testing::Test {
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

TEST_F(PutRecordsCheckSchemaTest, Valid) {
  saltfish::PutRecordsRequest req;
  source::Record *record{nullptr};
  record = req.add_records();
  record->add_reals(0.1234);
  record->add_reals(-852.32);
  record->add_cats("blue");

  record = req.add_records();
  record->add_reals(0.434);
  record->add_reals(-1052.32);
  record->add_cats("red");

  auto begin_recs = req.records().begin();
  auto end_recs = req.records().end();
  auto checked = put_records_check_schema(schema_, begin_recs, end_recs);
  EXPECT_TRUE(checked.first);
  EXPECT_EQ("", checked.second);
}

TEST_F(PutRecordsCheckSchemaTest, MissingFeature) {
  saltfish::PutRecordsRequest req;
  source::Record *record{nullptr};
  record = req.add_records();
  record->add_reals(0.1234);
  record->add_reals(-852.32);
  record->add_cats("blue");

  record = req.add_records();
  record->add_reals(0.434);
  record->add_cats("red");

  auto begin_recs = req.records().begin();
  auto end_recs = req.records().end();
  auto checked = put_records_check_schema(schema_, begin_recs, end_recs);
  EXPECT_FALSE(checked.first);
  EXPECT_NE("", checked.second);
}

TEST_F(PutRecordsCheckSchemaTest, TooManyFeatures) {
  saltfish::PutRecordsRequest req;
  source::Record *record{nullptr};
  record = req.add_records();
  record->add_reals(0.1234);
  record->add_reals(-852.32);
  record->add_cats("blue");

  record = req.add_records();
  record->add_reals(0.434);
  record->add_reals(-1052.32);
  record->add_cats("red");
  record->add_cats("yellow");

  auto begin_recs = req.records().begin();
  auto end_recs = req.records().end();
  auto checked = put_records_check_schema(schema_, begin_recs, end_recs);
  EXPECT_FALSE(checked.first);
  EXPECT_NE("", checked.second);
}

TEST_F(PutRecordsCheckSchemaTest, IncorrectFeatureType) {
  saltfish::PutRecordsRequest req;
  source::Record *record{nullptr};
  record = req.add_records();
  record->add_reals(0.1234);
  record->add_reals(-852.32);
  record->add_cats("blue");

  record = req.add_records();
  record->add_reals(0.434);
  record->add_cats("red");
  record->add_cats("yellow");

  auto begin_recs = req.records().begin();
  auto end_recs = req.records().end();
  auto checked = put_records_check_schema(schema_, begin_recs, end_recs);
  EXPECT_FALSE(checked.first);
  EXPECT_NE("", checked.second);
}

TEST_F(PutRecordsCheckSchemaTest, InvalidFeatureInSchema) {
  source::Schema invalid_schema = schema_;
  source::Feature* feat = invalid_schema.add_features();
  feat->set_name("problematic_feature");
  feat->set_feature_type(source::Feature::INVALID);

  saltfish::PutRecordsRequest req;
  auto begin_recs = req.records().begin();
  auto end_recs = req.records().end();
  auto checked = put_records_check_schema(invalid_schema, begin_recs, end_recs);
  EXPECT_FALSE(checked.first);
  EXPECT_THAT(checked.second, testing::HasSubstr("invalid"));
  EXPECT_THAT(checked.second, testing::HasSubstr("problematic_feature"));
}


int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  testing::FLAGS_gtest_color = "yes";
  return RUN_ALL_TESTS();
}
