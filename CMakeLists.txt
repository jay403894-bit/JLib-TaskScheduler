
cmake_minimum_required(VERSION 3.21)

set(JLIBSCHED_VERSION "1.2.0")

option(JLIBSCHED_STATS "Per-thread scheduler counters and timing histograms (diagnostic build)" OFF)
option(JLIBSCHED_RETRY_STATS "Record CAS retries per call -- max and buckets, not an average (diagnostic; sits inside CAS loops)" OFF)
option(JLIBSCHED_TSAN "Build with -fsanitize=thread (Linux/WSL only; see the fiber caveat above)" OFF)

set(JLIBSCHED_FAST_SPIN_TRIES "0" CACHE STRING "Plain-spin retries before a contended bare-thread lock starts helping (0 = measured optimum)")

set(JLIBSCHED_ARM_SPIN_HINT "isb" CACHE STRING "AArch64 spin hint: isb, yield or nop")
set_property(CACHE JLIBSCHED_ARM_SPIN_HINT PROPERTY STRINGS yield isb nop)

set(JLIBSCHED_STANDALONE OFF)
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    set(JLIBSCHED_STANDALONE ON)
endif()

if(JLIBSCHED_STANDALONE)
    if(WIN32)
        set(_jlibsched_win_arm OFF)
        if(CMAKE_GENERATOR_PLATFORM MATCHES "^(ARM64|arm64|aarch64)$")
            set(_jlibsched_win_arm ON)
        elseif(NOT CMAKE_GENERATOR_PLATFORM AND
               "$ENV{PROCESSOR_ARCHITECTURE}" MATCHES "^(ARM64|arm64|aarch64)$")
            set(_jlibsched_win_arm ON)
        endif()

        if(_jlibsched_win_arm)
            if(CMAKE_VERSION VERSION_LESS 3.26)
                message(FATAL_ERROR
                    "Windows on ARM64 needs CMake 3.26+ for the ASM_MARMASM language (armasm64). "
                    "Found ${CMAKE_VERSION}.")
            endif()
            project(JLibScheduler VERSION ${JLIBSCHED_VERSION} LANGUAGES CXX ASM_MARMASM)
        else()
            project(JLibScheduler VERSION ${JLIBSCHED_VERSION} LANGUAGES CXX ASM_MASM)
        endif()
    else()
        project(JLibScheduler VERSION ${JLIBSCHED_VERSION} LANGUAGES CXX ASM)
    endif()

    get_property(_multi GLOBAL PROPERTY GENERATOR_IS_MULTI_CONFIG)
    if(_multi)
        set(CMAKE_CONFIGURATION_TYPES "Debug;Development;Release" CACHE STRING "" FORCE)
    elseif(NOT CMAKE_BUILD_TYPE)
        set(CMAKE_BUILD_TYPE "Development" CACHE STRING "" FORCE)
    endif()

    set(CMAKE_CXX_FLAGS_DEVELOPMENT        "/O2 /Ob2 /Zi /DJLIB_DEVELOPMENT" CACHE STRING "")
    set(CMAKE_EXE_LINKER_FLAGS_DEVELOPMENT "/DEBUG /INCREMENTAL:NO"          CACHE STRING "")
    set(CMAKE_STATIC_LINKER_FLAGS_DEVELOPMENT ""                             CACHE STRING "")
    if(NOT MSVC)
        set(CMAKE_CXX_FLAGS_DEVELOPMENT "-O2 -g -DJLIB_DEVELOPMENT" CACHE STRING "" FORCE)
        set(CMAKE_EXE_LINKER_FLAGS_DEVELOPMENT "" CACHE STRING "" FORCE)
    endif()

    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>DLL")
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
endif()

if(WIN32 AND NOT MSVC)
    message(FATAL_ERROR "On Windows, JLib::Scheduler requires MSVC: the context switch is MASM.")
endif()
if(NOT WIN32 AND NOT CMAKE_SYSTEM_NAME MATCHES "^(Linux|Android)$")
    message(FATAL_ERROR
        "JLib::Scheduler supports Windows and Linux/Android (x86-64 and AArch64). "
        "${CMAKE_SYSTEM_NAME} is not supported.")
endif()

if(WIN32)
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(ARM64|arm64|aarch64)$" OR CMAKE_GENERATOR_PLATFORM MATCHES "^(ARM64|arm64)$")
        set(JLIBSCHED_ARCH "aarch64")
    else()
        set(JLIBSCHED_ARCH "x86_64")
    endif()
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64|AMD64)$")
    set(JLIBSCHED_ARCH "x86_64")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64|arm64|ARM64)$")
    set(JLIBSCHED_ARCH "aarch64")
else()
    message(FATAL_ERROR
        "JLib::Scheduler has no context switch for CMAKE_SYSTEM_PROCESSOR='${CMAKE_SYSTEM_PROCESSOR}'. "
        "Supported: x86-64 and AArch64. Porting means adding src/posix/<arch>/{ContextSwitch.s,"
        "FiberInit.cpp} -- an ABI's worth of work, not a compiler flag.")
endif()

file(GLOB JLIBSCHED_SRC CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

foreach(_stale ContextSwitch.s ContextSwitch.S FiberInit.cpp)
    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/src/posix/${_stale}")
        message(FATAL_ERROR
            "Stale src/posix/${_stale} found. Arch-specific sources moved into src/posix/<arch>/; "
            "this copy would be globbed in ALONGSIDE the correct one and silently win at link "
            "time. Delete it (a sync without --delete is the usual cause).")
    endif()
endforeach()

if(WIN32)
    file(GLOB JLIBSCHED_PLAT CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/src/win32/*.cpp")

    if(JLIBSCHED_ARCH STREQUAL "aarch64")
        list(REMOVE_ITEM JLIBSCHED_PLAT "${CMAKE_CURRENT_SOURCE_DIR}/src/win32/FiberInit.cpp")

        list(APPEND JLIBSCHED_PLAT "${CMAKE_CURRENT_SOURCE_DIR}/src/posix/aarch64/FiberInit.cpp")

        set(JLIBSCHED_ASM "${CMAKE_CURRENT_SOURCE_DIR}/src/win32/aarch64/ContextSwitch.asm")
    else()
        set(JLIBSCHED_ASM "${CMAKE_CURRENT_SOURCE_DIR}/src/win32/ContextSwitch.asm")
    endif()
    if(JLIBSCHED_ARCH STREQUAL "aarch64")
        set_source_files_properties("${JLIBSCHED_ASM}" PROPERTIES LANGUAGE ASM_MARMASM)
    else()
        set_source_files_properties("${JLIBSCHED_ASM}" PROPERTIES LANGUAGE ASM_MASM)
    endif()
else()
    set(JLIBSCHED_OSDIR "posix")
    file(GLOB JLIBSCHED_PLAT CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/src/${JLIBSCHED_OSDIR}/*.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/src/posix/${JLIBSCHED_ARCH}/*.cpp")
    set(JLIBSCHED_ASM "${CMAKE_CURRENT_SOURCE_DIR}/src/posix/${JLIBSCHED_ARCH}/ContextSwitch.S")
endif()

if(WIN32)
    message(STATUS "JLib::Scheduler: ${CMAKE_SYSTEM_NAME} / ${JLIBSCHED_ARCH} (os layer: win32)")
else()
    message(STATUS "JLib::Scheduler: ${CMAKE_SYSTEM_NAME} / ${JLIBSCHED_ARCH} (os layer: ${JLIBSCHED_OSDIR})")
endif()

if(JLIBSCHED_TSAN)
    if(MSVC)
        message(FATAL_ERROR "JLIBSCHED_TSAN: MSVC has no thread sanitizer. Build under WSL/Linux.")
    endif()
    add_compile_options(-fsanitize=thread -g -O1 -fno-omit-frame-pointer)
    add_compile_definitions(JLIB_TSAN=1)
    add_link_options(-fsanitize=thread)
    message(STATUS "JLib::Scheduler: THREAD SANITIZER ON -- findings on FIBER paths are unproven "
                   "until __tsan_switch_to_fiber annotations exist; see the option's comment")
endif()

add_library(Scheduler STATIC ${JLIBSCHED_SRC} ${JLIBSCHED_PLAT} ${JLIBSCHED_ASM})
add_library(JLib::Scheduler ALIAS Scheduler)

if(JLIBSCHED_STATS)
    target_compile_definitions(Scheduler PUBLIC JLIBSCHED_STATS=1)
    message(STATUS "JLib::Scheduler: stats build ON (diagnostic build, not for perf numbers)")
endif()
if(JLIBSCHED_RETRY_STATS)
    target_compile_definitions(Scheduler PUBLIC JLIBSCHED_RETRY_STATS=1)
    message(STATUS "JLib::Scheduler: CAS retry distribution ON (diagnostic build, not for perf numbers)")
endif()

if(JLIBSCHED_ARM_SPIN_HINT STREQUAL "yield")
    target_compile_definitions(Scheduler PUBLIC JLIB_SPIN_HINT_YIELD=1)
    message(STATUS "JLib::Scheduler: AArch64 spin hint = yield (measured ~50%% slower on the contended path than the isb default)")
elseif(JLIBSCHED_ARM_SPIN_HINT STREQUAL "nop")
    target_compile_definitions(Scheduler PUBLIC JLIB_SPIN_HINT_NOP=1)
    message(STATUS "JLib::Scheduler: AArch64 spin hint = nop sled (control; default is yield)")
elseif(NOT JLIBSCHED_ARM_SPIN_HINT STREQUAL "isb")
    message(FATAL_ERROR "JLIBSCHED_ARM_SPIN_HINT must be isb, yield or nop (got: ${JLIBSCHED_ARM_SPIN_HINT})")
endif()

if(NOT JLIBSCHED_FAST_SPIN_TRIES MATCHES "^[0-9]+$")
    message(FATAL_ERROR "JLIBSCHED_FAST_SPIN_TRIES must be a non-negative integer (got: ${JLIBSCHED_FAST_SPIN_TRIES})")
endif()
target_compile_definitions(Scheduler PRIVATE JLIBSCHED_FAST_SPIN_TRIES=${JLIBSCHED_FAST_SPIN_TRIES})

option(JLIBSCHED_TUNABLE_FAST_SPIN "Expose the fast-spin bound as a runtime setting (diagnostic; for the lock bench)" OFF)
if(JLIBSCHED_TUNABLE_FAST_SPIN)
    target_compile_definitions(Scheduler PUBLIC JLIBSCHED_TUNABLE_FAST_SPIN=1)
    message(STATUS "JLib::Scheduler: fast-spin bound is RUNTIME-TUNABLE (diagnostic build, not for shipping)")
endif()

target_include_directories(Scheduler PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)
target_compile_features(Scheduler PUBLIC cxx_std_17)

option(JLIBSCHED_COROUTINES "Build the optional C++20 coroutine layer" OFF)
if(JLIBSCHED_COROUTINES)
    target_compile_features(Scheduler PUBLIC cxx_std_20)
    target_compile_definitions(Scheduler PUBLIC JLIBSCHED_COROUTINES=1)
    message(STATUS "JLib::Scheduler: C++20 coroutine layer ON")
endif()

if(WIN32)
    target_compile_definitions(Scheduler PUBLIC NOMINMAX WIN32_LEAN_AND_MEAN)
    target_link_libraries(Scheduler PUBLIC Synchronization)
else()
    find_package(Threads REQUIRED)
    target_link_libraries(Scheduler PUBLIC Threads::Threads)
endif()

if(WIN32 AND NOT JLIBSCHED_ARCH STREQUAL "aarch64")
    set(JLIBSCHED_CSBENCH_ASM "${CMAKE_CURRENT_SOURCE_DIR}/bench/win32/context_switch_variants.asm")
    set_source_files_properties("${JLIBSCHED_CSBENCH_ASM}" PROPERTIES LANGUAGE ASM_MASM)
    add_executable(SchedulerContextSwitchBench
        "${CMAKE_CURRENT_SOURCE_DIR}/bench/context_switch.cpp"
        "${JLIBSCHED_CSBENCH_ASM}")
    target_link_libraries(SchedulerContextSwitchBench PRIVATE Scheduler)
    target_compile_features(SchedulerContextSwitchBench PRIVATE cxx_std_17)
endif()

add_executable(SchedulerEpochVsMutex "${CMAKE_CURRENT_SOURCE_DIR}/bench/epoch_vs_mutex.cpp")
target_link_libraries(SchedulerEpochVsMutex PRIVATE Scheduler)
target_compile_features(SchedulerEpochVsMutex PRIVATE cxx_std_17)

add_executable(SchedulerEpochMechBench "${CMAKE_CURRENT_SOURCE_DIR}/bench/epoch_mechanisms.cpp")
target_link_libraries(SchedulerEpochMechBench PRIVATE Scheduler)
target_compile_features(SchedulerEpochMechBench PRIVATE cxx_std_17)

if(JLIBSCHED_TUNABLE_FAST_SPIN)
    add_executable(SchedulerLockBench "${CMAKE_CURRENT_SOURCE_DIR}/bench/lock_contention.cpp")
    target_link_libraries(SchedulerLockBench PRIVATE Scheduler)
    target_compile_features(SchedulerLockBench PRIVATE cxx_std_17)
endif()

add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/bench/compare")

add_executable(SchedulerDagBench "${CMAKE_CURRENT_SOURCE_DIR}/bench/dag_scaling.cpp")
target_link_libraries(SchedulerDagBench PRIVATE Scheduler)
target_compile_features(SchedulerDagBench PRIVATE cxx_std_17)

if(JLIBSCHED_COROUTINES)
    add_executable(SchedulerBench "${CMAKE_CURRENT_SOURCE_DIR}/bench/bench.cpp")
    target_link_libraries(SchedulerBench PRIVATE Scheduler)
    target_compile_features(SchedulerBench PRIVATE cxx_std_20)
    target_compile_definitions(SchedulerBench PRIVATE JLIBSCHED_VERSION_STRING="${JLIBSCHED_VERSION}")
else()
    message(STATUS "JLib::Scheduler: SchedulerBench skipped (needs -DJLIBSCHED_COROUTINES=ON)")
endif()

if(WIN32 AND JLIBSCHED_ARCH STREQUAL "aarch64")
    add_executable(SchedulerFiberAbiTest
        "${CMAKE_CURRENT_SOURCE_DIR}/tests/fibertest_win_aarch64.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/tests/win32/fibertest_probe_aarch64.asm")
    set_source_files_properties(
        "${CMAKE_CURRENT_SOURCE_DIR}/tests/win32/fibertest_probe_aarch64.asm"
        PROPERTIES LANGUAGE ASM_MARMASM)
    target_link_libraries(SchedulerFiberAbiTest PRIVATE Scheduler)
    target_compile_features(SchedulerFiberAbiTest PRIVATE cxx_std_17)
endif()

if(JLIBSCHED_STANDALONE)
    include(GNUInstallDirs)
    include(CMakePackageConfigHelpers)

    set(JLIBSCHED_INCDEST "${CMAKE_INSTALL_INCLUDEDIR}/JLibScheduler")

    if(CMAKE_CONFIGURATION_TYPES)
        set(JLIBSCHED_LIBDEST "${CMAKE_INSTALL_LIBDIR}/$<CONFIG>")
    else()
        set(JLIBSCHED_LIBDEST "${CMAKE_INSTALL_LIBDIR}")
    endif()

    install(TARGETS Scheduler EXPORT JLibSchedulerTargets
            ARCHIVE DESTINATION "${JLIBSCHED_LIBDEST}"
            INCLUDES DESTINATION "${JLIBSCHED_INCDEST}")
    install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/include/"
            DESTINATION "${JLIBSCHED_INCDEST}"
            FILES_MATCHING PATTERN "*.h")
    install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE" DESTINATION "${CMAKE_INSTALL_DOCDIR}")
    install(EXPORT JLibSchedulerTargets
            FILE JLibSchedulerTargets.cmake NAMESPACE JLib::
            DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/JLibScheduler")

    write_basic_package_version_file(
        "${CMAKE_CURRENT_BINARY_DIR}/JLibSchedulerConfigVersion.cmake"
        VERSION ${JLIBSCHED_VERSION} COMPATIBILITY SameMajorVersion)
    file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/JLibSchedulerConfig.cmake.in"
"@PACKAGE_INIT@\ninclude(CMakeFindDependencyMacro)\nif(NOT WIN32)\n  find_dependency(Threads)\nendif()\ninclude(\"\${CMAKE_CURRENT_LIST_DIR}/JLibSchedulerTargets.cmake\")\ncheck_required_components(JLibScheduler)\n")
    configure_package_config_file(
        "${CMAKE_CURRENT_BINARY_DIR}/JLibSchedulerConfig.cmake.in"
        "${CMAKE_CURRENT_BINARY_DIR}/JLibSchedulerConfig.cmake"
        INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/JLibScheduler")
    install(FILES "${CMAKE_CURRENT_BINARY_DIR}/JLibSchedulerConfig.cmake"
                  "${CMAKE_CURRENT_BINARY_DIR}/JLibSchedulerConfigVersion.cmake"
            DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/JLibScheduler")
endif()

if(WIN32)

endif()

option(JLIBSCHED_IO_LOCK_STATS "Count reactor mutex contention (diagnostic; perturbs the I/O path)" OFF)
if(JLIBSCHED_IO_LOCK_STATS)
    target_compile_definitions(Scheduler PRIVATE JLIBSCHED_IO_LOCK_STATS)
    message(STATUS "JLib::Scheduler: reactor lock stats ENABLED (perturbs the I/O path)")
endif()

if(MSVC)
else()
endif()

if(WIN32)
    add_executable(SchedulerFutexVariance "${CMAKE_CURRENT_SOURCE_DIR}/bench/futex_variance.cpp")
    target_link_libraries(SchedulerFutexVariance PRIVATE Scheduler)
    target_link_libraries(SchedulerFutexVariance PRIVATE winmm)
    target_compile_features(SchedulerFutexVariance PRIVATE cxx_std_17)
endif()

if(JLIB_FIBERREG_CTL)
endif()

if(JLIB_FIBERCHAIN_CTL)
endif()

if(JLIBSCHED_DAG_TERMINAL_TRACE)
  target_compile_definitions(Scheduler PUBLIC JLIBSCHED_DAG_TERMINAL_TRACE=1)
  message(STATUS "JLib::Scheduler: DAG terminal trace ON (diagnostic build)")
endif()

if(JLIBSCHED_DAGFIRE_CTL)
  target_compile_definitions(Scheduler PUBLIC JLIB_DAGFIRE_CTL_${JLIBSCHED_DAGFIRE_CTL}=1)
  message(STATUS "JLib::Scheduler: NEGATIVE CONTROL DAGFIRE ${JLIBSCHED_DAGFIRE_CTL} -- dag_priority_node_test MUST fail")
endif()

if(JLIBSCHED_FIBERHOLDER_CTL)
  target_compile_definitions(Scheduler PUBLIC JLIB_FIBERHOLDER_CTL_${JLIBSCHED_FIBERHOLDER_CTL}=1)
  message(STATUS "JLib::Scheduler: NEGATIVE CONTROL ${JLIBSCHED_FIBERHOLDER_CTL} -- the matching test MUST fail")
endif()

if(WIN32)
endif()

if(WIN32)
endif()

add_executable(SchedulerSubmitBench "${CMAKE_CURRENT_SOURCE_DIR}/bench/submit_bench.cpp")
target_link_libraries(SchedulerSubmitBench PRIVATE Scheduler)
target_compile_features(SchedulerSubmitBench PRIVATE cxx_std_17)

if(JLIBSCHED_COROUTINES)
    set(JLIBSCHED_TEST_STD cxx_std_20)
else()
    set(JLIBSCHED_TEST_STD cxx_std_17)
endif()

set(JLIBSCHED_TESTS
    SchedulerPoolRun          tests/pool_run.cpp
    SchedulerBatchTest        tests/batch_test.cpp
    SchedulerConfigScopeTest  tests/config_scope_test.cpp
    SchedulerSlabExitTest     tests/slab_thread_exit_test.cpp
    SchedulerDagUnsubmittedTest tests/dag_unsubmitted_test.cpp
    SchedulerSkipListPqTest   tests/skiplist_pq_test.cpp
    SchedulerMainAwayTest     tests/main_away_test.cpp
    SchedulerThreadScopeTest  tests/thread_scope_test.cpp
    SchedulerBlockInPlaceTest tests/block_in_place_test.cpp
    SchedulerFiberGrowTest    tests/fiber_grow_test.cpp
    SchedulerFtlTest          tests/ftl_test.cpp
    SchedulerHangHuntTest     tests/hang_hunt_test.cpp
    SchedulerKStealTest       tests/k_steal_test.cpp
    SchedulerLambdaSuspendTest tests/lambda_suspend_test.cpp
    SchedulerMainHelpTest     tests/main_help_test.cpp
    SchedulerMainPushToTest   tests/main_pushto_test.cpp
    SchedulerPeriodicTest     tests/periodic_test.cpp
    SchedulerTimerWakeTest    tests/timer_wake_test.cpp
    SchedulerPforAllocTest    tests/pfor_alloc_test.cpp
    SchedulerPforModeTest     tests/pfor_mode_test.cpp
    SchedulerInboxFairnessTest tests/inbox_fairness_test.cpp
    SchedulerPinTest          tests/pin_test.cpp
    SchedulerPinMainTest      tests/pin_main_test.cpp
    SchedulerReclaimTest      tests/reclaim_test.cpp
    SchedulerRecordTest       tests/record_test.cpp
    SchedulerSlotCheckTest    tests/slot_check_test.cpp)

if(JLIBSCHED_COROUTINES)
    list(APPEND JLIBSCHED_TESTS
        SchedulerCoroutineTest tests/coroutine_test.cpp
        SchedulerFutureTest    tests/future_test.cpp
        SchedulerSemCvTest     tests/semcv_test.cpp
        SchedulerLateFreeTest  tests/late_free_test.cpp
        SchedulerWaitAllocTest tests/wait_alloc_test.cpp)
    if(JLIBSCHED_STATS)
        list(APPEND JLIBSCHED_TESTS SchedulerStatsTest tests/stats_test.cpp)
    endif()
    if(WIN32)
        list(APPEND JLIBSCHED_TESTS SchedulerIoAsyncTest tests/io_async_test.cpp)
    endif()
endif()

list(LENGTH JLIBSCHED_TESTS _jlibsched_test_entries)
math(EXPR _jlibsched_test_last "${_jlibsched_test_entries} / 2 - 1")
foreach(i RANGE ${_jlibsched_test_last})
    math(EXPR _name_i "${i} * 2")
    math(EXPR _src_i "${i} * 2 + 1")
    list(GET JLIBSCHED_TESTS ${_name_i} _test_name)
    list(GET JLIBSCHED_TESTS ${_src_i} _test_src)
    add_executable(${_test_name} "${CMAKE_CURRENT_SOURCE_DIR}/${_test_src}")
    target_link_libraries(${_test_name} PRIVATE Scheduler)
    target_compile_features(${_test_name} PRIVATE ${JLIBSCHED_TEST_STD})
endforeach()
