# Create the main library
add_library(ovc ovc.cpp)

# Set target properties
target_include_directories(ovc PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:include>
)