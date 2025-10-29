set(CPM_USE_LOCAL_PACKAGES ON)
set(CPM_SOURCE_CACHE "${CMAKE_SOURCE_DIR}/.cpmsource")

function(need_dftracer_utils)
  if(NOT dftracer_utils_ADDED)
    CPMAddPackage(
      NAME dftracer_utils
      GITHUB_REPOSITORY rayandrew/dftracer-utils
      GIT_TAG a95053e60029508770bad4f8037aeae7e37d316d
      OPTIONS
        "DFTRACER_UTILS_BUILD_TESTS OFF"
        "DFTRACER_UTILS_BUILD_SHARED ON"
        "DFTRACER_UTILS_BUILD_BINARIES ON"
      )
  endif()
endfunction()
