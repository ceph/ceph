# based on: https://gist.github.com/matthew-d-jones/550ee6fa2f89610f88991513fa5d6cfc

# convert a hexadeciaml charecter to its numeric value
macro(HEXCHAR2DEC VAR VAL)
    if (${VAL} MATCHES "[0-9]")
        SET(${VAR} ${VAL})
    elseif(${VAL} MATCHES "[aA]")
        SET(${VAR} 10)
    elseif(${VAL} MATCHES "[bB]")
        SET(${VAR} 11)
    elseif(${VAL} MATCHES "[cC]")
        SET(${VAR} 12)
    elseif(${VAL} MATCHES "[dD]")
        SET(${VAR} 13)
    elseif(${VAL} MATCHES "[eE]")
        SET(${VAR} 14)
    elseif(${VAL} MATCHES "[fF]")
        SET(${VAR} 15)
    else()
        SET(${VAR} 0)
    endif()
endmacro(HEXCHAR2DEC)

# convert a hexadeciaml string to its numeric value
macro(HEX2DEC VAR VAL)
    SET(CURINDEX 0)
    STRING(LENGTH "${VAL}" CURLENGTH)
    SET(${VAR} 0)
    while (CURINDEX LESS CURLENGTH)
        STRING(SUBSTRING "${VAL}" ${CURINDEX} 1 CHAR)
        HEXCHAR2DEC(CHAR ${CHAR})
        MATH(EXPR POWAH "(1<<((${CURLENGTH}-${CURINDEX}-1)*4))")
        MATH(EXPR CHAR "(${CHAR}*${POWAH})")
        MATH(EXPR ${VAR} "${${VAR}}+${CHAR}")
        MATH(EXPR CURINDEX "${CURINDEX}+1")
    endwhile()
endmacro(HEX2DEC)

# convert a hexadeciaml version string to numerical version string
# last two hexadecimal charecters hold the pre-release id and are ignored
# E.g.: 000801ff = 0.8.1
# (in pre-release id, 0xff is the final release)
macro(HEX2DECVERSION DECVERSION HEXVERSION)
    STRING(SUBSTRING "${HEXVERSION}" 0 2 MAJOR_HEX)
    HEX2DEC(MAJOR_DEC ${MAJOR_HEX})
    STRING(SUBSTRING "${HEXVERSION}" 2 2 MINOR_HEX)
    HEX2DEC(MINOR_DEC ${MINOR_HEX})
    STRING(SUBSTRING "${HEXVERSION}" 4 2 REVISION_HEX)
    HEX2DEC(REVISION_DEC ${REVISION_HEX})
    SET(${DECVERSION} ${MAJOR_DEC}.${MINOR_DEC}.${REVISION_DEC})
endmacro(HEX2DECVERSION)


# version appear in the header file as a C macro with hexadecimal value
# E.g.: #define RD_KAFKA_VERSION  0x000801ff
macro(RDKafka_check_version)
 file(READ "${rdkafka_INCLUDE_DIR}/librdkafka/rdkafka.h" RDKafka_version_header)
    string(REGEX MATCH "define[ \t]+RD_KAFKA_VERSION[ \t]+0x+([a-fA-F0-9]+)" RDKafka_version_match "${RDKafka_version_header}")
    set(LIBRDKAKFA_VERSION_HEX "${CMAKE_MATCH_1}")
    HEX2DECVERSION(RDKafka_VERSION ${LIBRDKAKFA_VERSION_HEX})
    if(NOT RDKAfka_FIND_VERSION EQUAL "")
		if(${RDKafka_VERSION} VERSION_LESS ${RDKafka_FIND_VERSION})
            message(SEND_ERROR "librdkafka version ${RDKafka_VERSION} found, but at least version ${RDKafka_FIND_VERSION} is required")
            set(LIBRDKAKFA_VERSION_OK FALSE)
		endif()
	endif()
endmacro(RDKafka_check_version)

include(FindPackageHandleStandardArgs)

find_path(rdkafka_INCLUDE_DIR
  NAMES librdkafka/rdkafka.h)

set(LIBRDKAKFA_VERSION_OK TRUE)
RDKafka_check_version()

find_library(rdkafka_LIBRARY
  NAMES rdkafka)

find_package_handle_standard_args(RDKafka DEFAULT_MSG
  rdkafka_INCLUDE_DIR
  rdkafka_LIBRARY)

if(LIBRDKAKFA_VERSION_OK AND RDKafka_FOUND AND NOT (TARGET RDKafka::RDKafka))
  add_library(RDKafka::RDKafka UNKNOWN IMPORTED)
  set_target_properties(RDKafka::RDKafka PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${rdkafka_INCLUDE_DIR}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${rdkafka_LIBRARY}")
endif()
