#include <iostream>

#include <riakpp/connection.hpp>
#include <riakpp/client.hpp>

using namespace riak;
using namespace std;


int main(int argc, const char **argv) {
  riak::client client{"localhost", 10017};
  riak::object obj{"test", "val"};
  obj.value() = "some test data";
  client.store(obj, [](error_code err) {
      cout << err.message() << endl;
  });
  return 0;
}
