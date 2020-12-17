cmake_minimum_required(VERSION 3.17)
project(pro_6)

set(CMAKE_CXX_STANDARD 14)

include_directories(include)

add_executable(pro_6
        include/buffer_layer.h
        include/db.h
        include/disk_space_layer.h
        include/index_layer.h
        include/lock_manager.h
        src/buffer_layer.cpp
        src/db.cpp
        src/disk_space_layer.cpp
        src/index_layer.cpp
        src/lock_manager.cpp
        src/main.cpp
        Makefile)
