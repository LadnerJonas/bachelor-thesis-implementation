include(CTest)

find_package(GTest REQUIRED)
include_directories(../include)

add_executable(tests test_main.cpp
        slotted-page/page-implementation/test_LockFreeManagedSlottedPage.cpp
        slotted-page/page-implementation/test_ManagedSlottedPage.cpp
        slotted-page/page-implementation/test_RawSlottedPage.cpp
        slotted-page/page-manager/test_LockFreePageManager.cpp
        slotted-page/page-manager/test_OnDemandPageManager.cpp
        slotted-page/page-manager/test_OnDemandPageSingleThreadManager.cpp)
target_link_libraries(tests PRIVATE GTest::gtest GTest::gtest_main)

add_test(NAME ExecuteTests COMMAND execute_tests)
