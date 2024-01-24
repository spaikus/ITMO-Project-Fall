add_library(playground_lib INTERFACE)
target_include_directories(playground_lib INTERFACE include)

add_executable(playground playground.cpp)
target_link_libraries(playground 
    playground_lib
    Arrow::arrow_shared Parquet::parquet_shared)

add_executable(generate_data generate_data.cpp)
target_link_libraries(generate_data 
    playground_lib
    Arrow::arrow_shared Parquet::parquet_shared)
