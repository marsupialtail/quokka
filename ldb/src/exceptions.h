#include <iostream>
#pragma once
#define ASSERT_MSG(expr, msg) \
    do { \
        if (!(expr)) { \
            std::cerr << "Assertion failed: " << #expr << "\n" \
                      << "Error message: " << msg << "\n"; \
            std::terminate(); \
        } \
    } while (false);