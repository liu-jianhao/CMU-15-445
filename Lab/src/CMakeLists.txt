##################################################################################
# SRC CMAKELISTS
##################################################################################

# https://raw.githubusercontent.com/azadkuh/sqlite-amalgamation/master/CMakeLists.txt
string(REGEX MATCH "Clang"  IS_CLANG "${CMAKE_CXX_COMPILER_ID}")
string(REGEX MATCH "GNU"    IS_GNUXX "${CMAKE_CXX_COMPILER_ID}")
string(REGEX MATCH "Linux"  IS_LINUX "${CMAKE_SYSTEM_NAME}")
string(REGEX MATCH "Darwin" IS_MACOS "${CMAKE_SYSTEM_NAME}")


# sqlite3 compile options, modules, ...
option(BUILD_ENABLE_DBSTAT_VTAB "enables dbstat virtual table"                 OFF)
option(BUILD_ENABLE_FTS3        "enables full text searches version 3"         OFF)
option(BUILD_ENABLE_FTS5        "enables full text searches version 5"         OFF)
option(BUILD_ENABLE_ICU         "enables international components fir unicode" OFF)
option(BUILD_ENABLE_JSON1       "enables JSON SQL functins"                    OFF)
option(BUILD_ENABLE_RBU         "enables resumable bulk update extension"      OFF)
option(BUILD_ENABLE_RTREE       "enables R*TRee index extension"               OFF)

# other build options
option(BUILD_SHARED_LIBS "build sqlite3 as a unix shared (so/dylib) library" ON)
option(BUILD_SHELL       "build sqlite3 shell application"                   ON)
if(MSVC)
    option(BUILD_MT_RELEASE "static msvcrt build" ON)
endif()

# compiler settings
if(IS_CLANG OR IS_GNUXX)
    set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS} -O3 -g0 -Wall -Wextra -pedantic -Wcast-align -Wunused -Wno-unused-parameter")
elseif(MSVC)
    set(CMAKE_C_FLAGS_RELEASE "-nologo -Zc:wchar_t -FS -O2 -MD -Zc:strictStrings -W3")
    set(CMAKE_C_FLAGS_DEBUG   "${CMAKE_C_FLAGS_DEBUG} -MDd")
    if(BUILD_MT_RELEASE)
        string(REPLACE "-MD"  "-MT"  CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE}")
        string(REPLACE "-MDd" "-MTd" CMAKE_C_FLAGS_DEBUG   "${CMAKE_C_FLAGS_DEBUG}")
    endif()
    set(CMAKE_SHARED_LINKER_FLAGS_RELEASE
            "${CMAKE_SHARED_LINKER_FLAGS_RELASE} \
            /Gy /GF /OPT:REF /OPT:ICF")
    if(BUILD_SHARED_LIBS)
        message(SEND_ERROR "@error: shared lib is not supported under Visual Studio.\n \
                please compile as a static lib.")
    endif()
endif()

if(BUILD_ENABLE_DBSTAT_VTAB)
    add_definitions(-DSQLITE_ENABLE_DBSTAT_VTAB)
endif()
if(BUILD_ENABLE_FTS3)
    add_definitions(-DSQLITE_ENABLE_FTS3)
endif()
if(BUILD_ENABLE_FTS5)
    add_definitions(-DSQLITE_ENABLE_FTS5)
endif()
if(BUILD_ENABLE_ICU)
    add_definitions(-DSQLITE_ENABLE_ICU)
endif()
if(BUILD_ENABLE_JSON1)
    add_definitions(-DSQLITE_ENABLE_JSON1)
endif()
if(BUILD_ENABLE_RBU)
    add_definitions(-DSQLITE_ENABLE_RBU)
endif()
if(BUILD_ENABLE_RTREE)
    add_definitions(-DSQLITE_ENABLE_RTREE)
endif()
# Multi-thread.
# In this mode, SQLite can be safely used by multiple threads provided that no single
# database connection is used simultaneously in two or more threads.
add_definitions(-DSQLITE_THREADSAFE=2)

# sqlite3 as library
add_library(sqlite3 sqlite/sqlite3.c include/sqlite/sqlite3ext.h include/sqlite/sqlite3.h)
if(IS_LINUX)
    if(BUILD_SHARED_LIBS)
        target_link_libraries(sqlite3 pthread dl)
    endif()
elseif(IS_MACOS AND BUILD_SHARED_LIBS)
    set(CMAKE_SKIP_RPATH 0) # make dynamic linking work for Mac
endif()

# shell app
if(BUILD_SHELL)
    add_executable(sqlite3_shell sqlite/shell.c)
    target_link_libraries(sqlite3_shell sqlite3)
    set_target_properties(sqlite3_shell PROPERTIES OUTPUT_NAME sqlite3)

    if(IS_LINUX)
        if(NOT BUILD_SHARED_LIBS)
            target_link_libraries(sqlite3_shell pthread dl)
        endif()
    endif()
endif()

# --- [ sqlite_vtable
file(GLOB_RECURSE srcs ${PROJECT_SOURCE_DIR}/src/*/*.cpp)
file(GLOB sqlite_srcs ${PROJECT_SOURCE_DIR}/src/sqlite/*.c)
list(REMOVE_ITEM srcs ${sqlite_srcs})
add_library(vtable SHARED ${srcs})
