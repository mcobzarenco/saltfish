# execute_process(COMMAND which protoc-gen-cpp_rpcz OUTPUT_VARIABLE RPCZ_PLUGIN_CPP_BIN)
# execute_process(COMMAND which protoc-gen-py_rpcz OUTPUT_VARIABLE RPCZ_PLUGIN_PY_BIN)

function(PROTOBUF_GENERATE_RPCZ SRCS HDRS)
  PROTOBUF_GENERATE_MULTI(PLUGIN "cpp_rpcz" PROTOS ${ARGN}
                          OUTPUT_STRUCT "_SRCS:.rpcz.cc;_HDRS:.rpcz.h"
                          DEPENDS ${PLUGIN_BIN})
  set(${SRCS} ${_SRCS} PARENT_SCOPE)
  set(${HDRS} ${_HDRS} PARENT_SCOPE)
endfunction()

function(PROTOBUF_GENERATE_PYTHON_RPCZ SRCS)
  set(PLUGIN_BIN ${RPCZ_PLUGIN_ROOT}/python/protoc-gen-python_rpcz)
  PROTOBUF_GENERATE_MULTI(PLUGIN "python_rpcz" PROTOS ${ARGN}
                          OUTPUT_STRUCT "_SRCS:_rpcz.py"
                          DEPENDS ${PLUGIN_BIN})
  set(${SRCS} ${_SRCS} PARENT_SCOPE)
endfunction()

function(PROTOBUF_GENERATE_RPCZ_F SRCS HDRS OPTS)
  PROTOBUF_GENERATE_MULTI(PLUGIN "cpp_rpcz" PROTOS ${ARGN}
                          OUTPUT_STRUCT "_SRCS:.rpcz.cc;_HDRS:.rpcz.h"
                          FLAGS ${OPTS}
                          # FLAGS "--plugin=protoc-gen-cpp_rpcz=${RPCZ_PLUGIN_CPP_BIN}"
                          DEPENDS ${PLUGIN_BIN})
  set(${SRCS} ${_SRCS} PARENT_SCOPE)
  set(${HDRS} ${_HDRS} PARENT_SCOPE)
endfunction()

function(PROTOBUF_GENERATE_PYTHON_RPCZ_F SRCS OPTS)
  set(PLUGIN_BIN ${RPCZ_PLUGIN_ROOT}/python/protoc-gen-python_rpcz)
  PROTOBUF_GENERATE_MULTI(PLUGIN "python_rpcz" PROTOS ${ARGN}
                          OUTPUT_STRUCT "_SRCS:_rpcz.py"
                          FLAGS ${OPTS}
                          # FLAGS "--plugin=protoc-gen-python_rpcz=${RPCZ_PLUGIN_PY_BIN}"
                          DEPENDS ${PLUGIN_BIN})
  set(${SRCS} ${_SRCS} PARENT_SCOPE)
endfunction()
