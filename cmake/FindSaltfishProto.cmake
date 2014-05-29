# -*- cmake -*-

# - Find saltfish.proto
# Find the protobuf saltfish.proto (Saltfish API)
# This module defines
#  SALTFISHPROTO_FOUND
#  SALTFISHPROTO_PATH - where to find saltfish.proto
#  COMMON_PROTO_ROOT - common root for protos


SET(COMMON_PROTO_ROOT /usr/share/proto)

IF (EXISTS "${COMMON_PROTO_ROOT}/reinferio/saltfish.proto")
  SET(SALTFISHPROTO_PATH "reinferio/saltfish.proto")
  SET(SALTFISHPROTO_FOUND on)
ELSE ()
  SET(SALTFISHPROTO_FOUND off)
ENDIF ()

IF (SALTFISHPROTO_FOUND)
   IF (NOT SaltfishProto_FIND_QUIETLY)
      MESSAGE(STATUS "Found saltfish proto: ${SALTFISHPROTO_PATH}")
   ENDIF ()
ELSE ()
   IF (SaltfishProto_FIND_REQUIRED)
      MESSAGE(FATAL_ERROR "Could not find saltfish.proto - "
        "is saltfish-proto installed?")
   ENDIF ()
ENDIF ()

MARK_AS_ADVANCED(
  COMMON_PROTO_ROOT
  SALTFISHPROTO_FOUND
  SALTFISHPROTO_PATH
)
