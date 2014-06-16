add_library(treadmill
    summarizer.hpp
    moments_summarizer.hpp
    moments_summarizer.cpp
    exact_quantile_summarizer.hpp
    exact_quantile_summarizer.cpp
    categorical_histogram_summarizer.hpp
    categorical_histogram_summarizer.cpp)
target_link_libraries(treadmill ${JSONCPP_LIBRARY})
