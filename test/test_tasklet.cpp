#include "tasklet.hpp"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <zmq.hpp>

#include <algorithm>
#include <atomic>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

using namespace std;
using namespace reinferio;


TEST(TaskletTest, GenerateId) {
  const uint32_t N = 100;
  constexpr char hex[] = "0123456789abcdef";
  const unordered_set<char> hex_chars{hex, hex + sizeof(hex)};
  unordered_set<char> chars;
  for (uint32_t width = 10; width < N + 10; ++width) {
    const string id{lib::generate_id(width)};
    chars.insert(id.begin(), id.end());
    EXPECT_EQ(width, id.size()) << id;
    EXPECT_TRUE(all_of(id.begin(), id.end(), [&hex_chars](const char c) {
          return hex_chars.count(c);
        })) << id;
  }
}

TEST(TaskletTest, CallsHandlersCorrectly) {
  int set_up_called{0};
  int tear_down_called{0};
  int handler_called{0};
  auto set_up = [&]() { ++set_up_called; };
  auto tear_down = [&]() { ++tear_down_called; };

  zmq::context_t context{1};
  auto handler = [&](const string s) {
    ++handler_called;
    return s + s;
  };
  {
    lib::Tasklet task{context, set_up, tear_down};
    auto conn = task.connect(handler);
    EXPECT_EQ(0, handler_called);

    EXPECT_EQ("abcabc", conn("abc"));
    EXPECT_EQ(1, set_up_called);
    EXPECT_EQ(1, handler_called);

    EXPECT_EQ("", conn(""));
    EXPECT_EQ(2, handler_called);
    EXPECT_EQ(0, tear_down_called);
  }
  EXPECT_EQ(1, tear_down_called);
}

TEST(TaskletTest, CallsHandlersCorrectlyManyThreads) {
  constexpr int N_THREADS{10};
  atomic<int> set_up_called{0};
  atomic<int> tear_down_called{0};
  atomic<int> handler_called{0};
  auto set_up = [&]() { ++set_up_called; };
  auto tear_down = [&]() { ++tear_down_called; };

  zmq::context_t context{1};
  auto handler = [&](const string s) {
    ++handler_called;
    return s + s;
  };
  vector<thread> threads;
  {
    lib::Tasklet task{context, set_up, tear_down};
    int count{0};
    while(++count <= N_THREADS) {
      threads.emplace_back([&]() {
          auto conn = task.connect(handler);
          EXPECT_EQ("abcabc", conn("abc"));
          EXPECT_EQ(1, set_up_called);
          EXPECT_EQ("", conn(""));
          EXPECT_EQ(0, tear_down_called);
        });
    }
    for (auto& t : threads) t.join();
    EXPECT_EQ(2 * N_THREADS, handler_called);
  }
  EXPECT_EQ(1, tear_down_called);
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  testing::FLAGS_gtest_color = "yes";
  google::InitGoogleLogging(argv[0]);
  google::LogToStderr();
  return RUN_ALL_TESTS();
}
