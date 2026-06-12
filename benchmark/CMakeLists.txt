# Jetton TPS benchmark tooling (see benchmark/DESIGN.md)

add_executable(bench-state-gen state-gen.cpp common.cpp common.h)
target_include_directories(bench-state-gen PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
# tddb links the vendored rocksdb PUBLICly (raw rocksdb:: is used for
# SstFileWriter / IngestExternalFile / Checkpoint).
target_link_libraries(bench-state-gen PRIVATE tdutils ton_crypto ton_block ton_db tddb tl_api tl-utils)
