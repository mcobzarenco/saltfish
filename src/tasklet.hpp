#ifndef REINFERIO_LIB_TASLKET_HPP
#define REINFERIO_LIB_TASLKET_HPP

#include <zmq.hpp>
#include <glog/logging.h>

#include <chrono>
#include <cstring>
#include <functional>
#include <random>
#include <string>
#include <thread>
#include <type_traits>


namespace reinferio { namespace lib {

class Tasklet;
template<typename Handler>
class Connection;

using Closure = std::function<void(void)>;

inline std::string zmq_msg_to_str(const zmq::message_t& msg) {
  return std::string{reinterpret_cast<const char *>(msg.data()), msg.size()};
}

inline std::string generate_id(const uint32_t width=32) {
  static const char hex[] = "0123456789abcdef";
  auto seed = std::chrono::system_clock::now().time_since_epoch().count();
  seed += *reinterpret_cast<size_t*>(&seed);
  std::default_random_engine gen(seed);
  std::uniform_int_distribution<int> dist(0,15);
  std::string id;
  id.reserve(width + 1);
  for(uint32_t i = 0; i < width; ++i)  id.push_back(hex[dist(gen)]);
  return id;
}

inline void task_loop(zmq::socket_t socket, Closure set_up, Closure tear_down);

class Tasklet {
 public:
  inline Tasklet(zmq::context_t& context,
                 Closure set_up = Tasklet::do_nothing,
                 Closure tear_down = Tasklet::do_nothing);
  Tasklet(const Tasklet&) = delete;
  Tasklet& operator=(const Tasklet&) = delete;

  virtual ~Tasklet() { if(!stopped_) stop(); }

  template<typename Handler>
  inline Connection<Handler> connect(Handler handler);
  inline const std::string& endpoint() const { return endpoint_; }
  inline void stop();

  static void do_nothing() {}
 protected:
  zmq::context_t& context_;
  const std::string endpoint_;
  std::unique_ptr<std::thread> worker_;
  bool stopped_;
};

Tasklet::Tasklet(zmq::context_t& context, Closure set_up, Closure tear_down)
    : context_{context},
      endpoint_{"inproc://" + generate_id()},
      stopped_{false} {
  zmq::socket_t socket{context, ZMQ_REP};
  socket.bind(endpoint_.c_str());
  worker_.reset(new std::thread{task_loop, std::move(socket),
          std::move(set_up), std::move(tear_down)});
}

template<typename Handler>
Connection<Handler> Tasklet::connect(Handler handler) {
  CHECK(!stopped_) << "Task is stopped, cannot connect to it";
  Connection<Handler> conn{context_, std::move(handler)};
  conn.connect(*this);
  return conn;
}

void Tasklet::stop() {
  if (!stopped_) {
    zmq::socket_t socket{context_, ZMQ_REQ};
    zmq::message_t empty;
    socket.connect(endpoint_.c_str());
    socket.send(empty);
    worker_->join();
    stopped_ = true;
  }
}

void task_loop(zmq::socket_t socket, Closure set_up, Closure tear_down) {
  set_up();
  while (true) {
    zmq::message_t request;
    try {
      socket.recv(&request);  // allow error_t to kill the thread
    } catch(const zmq::error_t& error) {
      LOG(WARNING) << "Tasklet - could not receive message.. exiting";
      tear_down();
      return;
    }
    if (request.size() == 0) break;
    auto handler = *reinterpret_cast<Closure* const *>(request.data());
    (*handler)();
    try {
      zmq::message_t empty;
      socket.send(empty);
    } catch(const zmq::error_t& error) {
      LOG(WARNING) << "Tasklet - REQ has went away";
    }
  }
  tear_down();
}

template<typename Handler>
class Connection {
 public:
  Connection(zmq::context_t& context, Handler handler);
  inline void connect(const Tasklet& tasklet);

  template<typename ...Args>
  auto operator()(Args&& ...args) ->
      decltype(std::declval<Handler>()(std::forward<Args>(args)...));

  const std::vector<std::string>& endpoints() const { return endpoints_; }
 protected:
  zmq::socket_t socket_;
  Handler handler_;
  std::vector<std::string> endpoints_;
};

template<typename Handler>
Connection<Handler>::Connection(zmq::context_t& context, Handler handler)
    : socket_{context, ZMQ_REQ}, handler_{std::move(handler)} { };

template<typename Handler>
inline void Connection<Handler>::connect(const Tasklet& tasklet) {
  socket_.connect(tasklet.endpoint().c_str());
  endpoints_.emplace_back(tasklet.endpoint());
}

template<typename Handler>
template<typename ...Args>
auto Connection<Handler>::operator()(Args&& ...args) ->
    decltype(std::declval<Handler>()(std::forward<Args>(args)...)) {
  using return_type =
      decltype(std::declval<Handler>()(std::forward<Args>(args)...));
  std::unique_ptr<return_type> result;
  Closure closure = [&] {
    result.reset(new return_type{handler_(std::forward<Args>(args)...)});
  };
  const Closure* closure_ptr = &closure;
  zmq::message_t request_msg{sizeof(const Closure*)};
  std::memcpy(request_msg.data(), reinterpret_cast<const void*>(&closure_ptr),
              sizeof(const Closure*));
  socket_.send(request_msg);
  zmq::message_t resp_msg;
  socket_.recv(&resp_msg);
  return std::move(*result);
}

}}  // namespace reinferio::lib

#endif  // REINFERIO_LIB_TASKLET_HPP
