add_subdirectory(consensus)

add_executable(test-ext-message-pool test-ext-message-pool.cpp)

target_link_libraries(test-ext-message-pool PRIVATE
    tdutils
    tdactor
    ton_crypto
    ton_validator
    validator
    td-test-main
)

ton_test(test-ext-message-pool)
