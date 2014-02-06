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
  auto set_up = [&]() { ++set_up_called; };
  auto tear_down = [&]() { ++tear_down_called; };

  zmq::context_t context{1};
  int twice_called{0};
  int add_called{0};
  int unique_called{0};
  auto twice_hdl = [&](const string s) { ++twice_called; return s + s; };
  auto add_hdl = [&](int a, int b) { ++add_called; return a + b; };
  auto unique_hdl = [&](char c) {
    ++unique_called;
    return unique_ptr<char>(new char{c});
  };
  {
    lib::Tasklet task{context, set_up, tear_down};
    auto twice = task.connect(twice_hdl);
    auto add = task.connect(add_hdl);
    auto unique = task.connect(unique_hdl);
    EXPECT_EQ(0, twice_called);
    EXPECT_EQ(0, add_called);
    EXPECT_EQ(0, unique_called);

    EXPECT_EQ("abcabc", twice("abc"));
    EXPECT_EQ("象形字!!iàn象形字!!iàn", twice("象形字!!iàn"));
    EXPECT_EQ(7, add(3, 4));
    EXPECT_EQ(34, add(30, 4));
    EXPECT_EQ('x', *unique('x'));
    EXPECT_EQ(1, set_up_called);
    EXPECT_EQ(2, twice_called);
    EXPECT_EQ(2, add_called);
    EXPECT_EQ(1, unique_called);

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

namespace {
class NoncopyableCounterConcat {
 public:
  NoncopyableCounterConcat(int* p) : p_{p} {}
  NoncopyableCounterConcat(const NoncopyableCounterConcat&) = delete;
  NoncopyableCounterConcat& operator=(const NoncopyableCounterConcat&) = delete;

  NoncopyableCounterConcat(NoncopyableCounterConcat&&) = default;

  string operator()(const string& s) { (*p_)++; return s + s; }
 private:
  int* p_;
};
}

TEST(TaskletTest, MovableOnlyHandlers) {
  int set_up_called{0};
  int tear_down_called{0};
  int handler1_called{0};
  int handler2_called{0};
  auto set_up = [&]() { ++set_up_called; };
  auto tear_down = [&]() { ++tear_down_called; };

  zmq::context_t context{1};
  NoncopyableCounterConcat handler1(&handler1_called);
  NoncopyableCounterConcat handler2(&handler2_called);
  {
    lib::Tasklet task{context, set_up, tear_down};
    auto conn1 = task.connect(std::move(handler1));
    EXPECT_EQ(0, handler1_called);

    auto conn2 = task.connect(std::move(handler2));
    EXPECT_EQ(0, handler2_called);

    EXPECT_EQ("abcabc", conn1("abc"));
    EXPECT_EQ("象形字 xiàngxíng象形字 xiàngxíng", conn1("象形字 xiàngxíng"));
    EXPECT_EQ(1, set_up_called);
    EXPECT_EQ(2, handler1_called);
    EXPECT_EQ(0, handler2_called);

    EXPECT_EQ("", conn1(""));
    EXPECT_EQ(3, handler1_called);
    EXPECT_EQ(0, handler2_called);

    EXPECT_EQ("xyzxyz", conn2("xyz"));
    EXPECT_EQ(3, handler1_called);
    EXPECT_EQ(1, handler2_called);
    EXPECT_EQ(0, tear_down_called);
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
