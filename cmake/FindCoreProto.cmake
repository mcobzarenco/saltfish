# -*- cmake -*-

# - Find core.proto
# Find the protobuf core.proto (core structures)
# This module defines
#  COREPROTO_FOUND
#  COREPROTO_PATH - where to find core.proto
#  COMMON_PROTO_ROOT - common root for protos


SET(COMMON_PROTO_ROOT /usr/share/proto)

IF (EXISTS "${COMMON_PROTO_ROOT}/reinferio/core.proto")
  SET(COREPROTO_PATH "${COMMON_PROTO_ROOT}/reinferio/core.proto")
  SET(COREPROTO_FOUND on)
ELSE ()
  SET(COREPROTO_FOUND off)
ENDIF ()

IF (COREPROTO_FOUND)
   IF (NOT CoreProto_FIND_QUIETLY)
      MESSAGE(STATUS "Found core proto: ${COREPROTO_PATH}")
   ENDIF ()
ELSE ()
   IF (CoreProto_FIND_REQUIRED)
      MESSAGE(FATAL_ERROR "Could not find core.proto - "
        "is core-proto installed?")
   ENDIF ()
ENDIF ()


MARK_AS_ADVANCED(
  COMMON_PROTO_ROOT
  COREPROTO_FOUND
  COREPROTO_PATH
)
