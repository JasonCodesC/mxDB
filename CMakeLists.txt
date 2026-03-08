cmake_minimum_required(VERSION 3.24)

project(mxdb LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(MXDB_ENABLE_WERROR "Treat warnings as errors" OFF)

include(CTest)

find_package(SQLite3 REQUIRED)
find_package(Threads REQUIRED)
find_package(Python3 REQUIRED COMPONENTS Interpreter)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
  add_compile_options(-Wall -Wextra -Wpedantic)
  if(MXDB_ENABLE_WERROR)
    add_compile_options(-Werror)
  endif()
endif()

add_library(mxdb_common
  engine/common/status/status.cc
  engine/common/logging/logging.cc
  engine/common/config/config.cc
  engine/common/crc32/crc32.cc
)
target_include_directories(mxdb_common PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(mxdb_common PUBLIC Threads::Threads)

add_library(mxdb_catalog
  engine/catalog/metadata_store.cc
)
target_include_directories(mxdb_catalog PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(mxdb_catalog PUBLIC mxdb_common SQLite::SQLite3)

add_library(mxdb_validation
  engine/validation/validator.cc
)
target_include_directories(mxdb_validation PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(mxdb_validation PUBLIC mxdb_common mxdb_catalog)

add_library(mxdb_wal
  engine/wal/wal_record.cc
  engine/wal/wal_writer.cc
  engine/wal/wal_reader.cc
)
target_include_directories(mxdb_wal PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(mxdb_wal PUBLIC mxdb_common)

add_library(mxdb_manifest
  engine/manifest/manifest.cc
)
target_include_directories(mxdb_manifest PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(mxdb_manifest PUBLIC mxdb_common)

add_library(mxdb_segment
  engine/segment/segment_store.cc
)
target_include_directories(mxdb_segment PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(mxdb_segment PUBLIC mxdb_common mxdb_wal mxdb_manifest)

add_library(mxdb_checkpoint
  engine/checkpoint/checkpoint_manager.cc
)
target_include_directories(mxdb_checkpoint PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(mxdb_checkpoint PUBLIC mxdb_common)

add_library(mxdb_admin
  engine/admin/admin_service.cc
)
target_include_directories(mxdb_admin PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(mxdb_admin PUBLIC mxdb_common mxdb_storage)

add_library(mxdb_storage
  engine/storage/feature_engine.cc
)
target_include_directories(mxdb_storage PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(mxdb_storage PUBLIC mxdb_common mxdb_catalog mxdb_validation mxdb_wal mxdb_manifest mxdb_segment mxdb_checkpoint)

add_library(mxdb_recovery
  engine/recovery/recovery_manager.cc
)
target_include_directories(mxdb_recovery PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(mxdb_recovery PUBLIC mxdb_common mxdb_storage mxdb_wal)

add_executable(featured
  server/process/main.cc
)
target_link_libraries(featured PRIVATE mxdb_common mxdb_catalog mxdb_storage mxdb_recovery mxdb_admin)

add_executable(featurectl
  tools/featurectl/main.cc
)
target_link_libraries(featurectl PRIVATE mxdb_common mxdb_catalog mxdb_storage mxdb_recovery mxdb_admin)

if(BUILD_TESTING)
  add_executable(test_metadata tests/unit/test_metadata.cc)
  target_link_libraries(test_metadata PRIVATE mxdb_catalog mxdb_validation)
  add_test(NAME test_metadata COMMAND test_metadata)

  add_executable(test_wal tests/unit/test_wal.cc)
  target_link_libraries(test_wal PRIVATE mxdb_wal)
  add_test(NAME test_wal COMMAND test_wal)

  add_executable(test_temporal_semantics tests/unit/test_temporal_semantics.cc)
  target_link_libraries(test_temporal_semantics PRIVATE mxdb_storage)
  add_test(NAME test_temporal_semantics COMMAND test_temporal_semantics)

  add_executable(test_end_to_end tests/integration/test_end_to_end.cc)
  target_link_libraries(test_end_to_end PRIVATE mxdb_catalog mxdb_storage mxdb_recovery mxdb_admin)
  add_test(NAME test_end_to_end COMMAND test_end_to_end)

  add_executable(test_checkpoint_recovery tests/recovery/test_checkpoint_recovery.cc)
  target_link_libraries(test_checkpoint_recovery PRIVATE mxdb_catalog mxdb_storage mxdb_recovery mxdb_admin)
  add_test(NAME test_checkpoint_recovery COMMAND test_checkpoint_recovery)

  add_executable(test_compaction_equivalence tests/integration/test_compaction_equivalence.cc)
  target_link_libraries(test_compaction_equivalence PRIVATE mxdb_catalog mxdb_storage mxdb_recovery mxdb_admin)
  add_test(NAME test_compaction_equivalence COMMAND test_compaction_equivalence)

  add_executable(test_admin_ops tests/integration/test_admin_ops.cc)
  target_link_libraries(test_admin_ops PRIVATE mxdb_catalog mxdb_storage mxdb_recovery mxdb_admin)
  add_test(NAME test_admin_ops COMMAND test_admin_ops)

  add_test(
    NAME test_python_sdk
    COMMAND ${Python3_EXECUTABLE} -m unittest discover -v -s ${CMAKE_CURRENT_SOURCE_DIR}/sdk/python/tests -p test_*.py
  )
  set_tests_properties(
    test_python_sdk
    PROPERTIES
      ENVIRONMENT "PYTHONPATH=${CMAKE_CURRENT_SOURCE_DIR}/sdk/python/src;FEATURECTL_BIN=${CMAKE_CURRENT_BINARY_DIR}/featurectl"
  )
endif()
