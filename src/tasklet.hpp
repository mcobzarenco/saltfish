#ifndef REINFERIO_LIB_TASLKET_HPP
#define REINFERIO_LIB_TASLKET_HPP

#include <zmq.hpp>
#include <glog/logging.h>

#include <chrono>
#include <cstring>
#include <functional>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <type_traits>


namespace reinferio { namespace lib {

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

template<typename Request, typename Response, typename Handler,
         typename SetUpHook, typename TearDownHook>
void task_loop(zmq::socket_t socket, Handler handler,
               SetUpHook set_up, TearDownHook tear_down);

template<typename Request, typename Response>
class Tasklet {
 public:
  class Connection {
   public:
    inline Response operator()(const Request& request);
   protected:
    inline Connection(zmq::context_t& context, const std::string endpoint);

    zmq::socket_t socket_;
    const std::string endpoint_;

    friend class Tasklet;
  };

  template<typename Handler,
           typename SetUpHook=void(*)(), typename TearDownHook=void(*)()>
  inline Tasklet(zmq::context_t& context,
                 Handler handler,
                 SetUpHook set_up = Tasklet<Request, Response>::do_nothing,
                 TearDownHook tear_down = Tasklet<Request, Response>::do_nothing);
  Tasklet(const Tasklet&) = delete;
  Tasklet& operator=(const Tasklet&) = delete;

  virtual ~Tasklet() { if(!stopped_) stop(); }

  inline Connection connect();
  inline const std::string& endpoint() { return endpoint_; }
  inline void stop();

  static void do_nothing() {}
 protected:
  zmq::context_t& context_;
  const std::string endpoint_;
  std::unique_ptr<std::thread> worker_;
  bool stopped_;
};

template<typename Request, typename Response>
Tasklet<Request, Response>::Connection::Connection(
    zmq::context_t& context, const std::string endpoint)
    : socket_{context, ZMQ_REQ}, endpoint_{std::move(endpoint)} {
  socket_.connect(endpoint_.c_str());
}

template<typename Request, typename Response>
Response Tasklet<Request, Response>::Connection::operator()(
    const Request& request) {
  const Request* reqptr = &request;
  zmq::message_t request_msg{sizeof(const Request*)};
  std::memcpy(request_msg.data(), reinterpret_cast<const void*>(&reqptr),
              sizeof(const Request*));
  socket_.send(request_msg);
  zmq::message_t resp_msg;
  socket_.recv(&resp_msg);
  std::unique_ptr<Response> resp{
    *reinterpret_cast<Response* const *>(resp_msg.data())};
  return Response{*resp};
}

template<typename Request, typename Response>
template<typename Handler, typename SetUpHook, typename TearDownHook>
Tasklet<Request, Response>::Tasklet(
    zmq::context_t& context, Handler handler,
    SetUpHook set_up, TearDownHook tear_down)
    : context_{context},
      endpoint_{"inproc://" + generate_id()},
      stopped_{false} {
  zmq::socket_t socket{context, ZMQ_REP};
  socket.bind(endpoint_.c_str());
  worker_.reset(new std::thread{
      task_loop<Request, Response, Handler, SetUpHook, TearDownHook>,
          std::move(socket), std::move(handler),
          std::move(set_up), std::move(tear_down)});
}

template<typename Request, typename Response>
typename Tasklet<Request, Response>::Connection
Tasklet<Request, Response>::connect() {
  CHECK(!stopped_) << "Task is stopped, cannot connect to it";
  return Connection{context_, endpoint_.c_str()};
}

template<typename Request, typename Response>
void Tasklet<Request, Response>::stop() {
  zmq::socket_t socket{context_, ZMQ_REQ};
  zmq::message_t empty;
  socket.connect(endpoint_.c_str());
  socket.send(empty);
  worker_->join();
  stopped_ = true;
}

template<typename Request, typename Response, typename Handler,
         typename SetUpHook, typename TearDownHook>
void task_loop(zmq::socket_t socket, Handler handler,
                SetUpHook set_up, TearDownHook tear_down) {
  set_up();
  while (true) {
    zmq::message_t request;
    socket.recv(&request);  // allow error_t to kill the thread
    if (request.size() == 0) break;
    try {
      std::unique_ptr<Response> resp{new Response{
          std::move(handler(**reinterpret_cast<Request* const *>(request.data())))}};
      Response* respptr{resp.get()};
      zmq::message_t reply{sizeof(Response*)};
      std::memcpy(reply.data(), reinterpret_cast<const void*>(&respptr),
                  sizeof(Response*));
      socket.send(reply);
      resp.release();
    } catch(const zmq::error_t& error) {
      LOG(WARNING) << "Tasklet - REQ has went away";
    }
  }
  tear_down();
}

}}  // namespace reinferio::lib

#endif  // REINFERIO_LIB_TASKLET_HPP
