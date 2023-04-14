// some crazy shit

#pragma once

#include <cmath>
#include <memory>
#include <vector>
#include <assert.h>
#include "bridge.h"
#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>


namespace py = pybind11;

class CEP {
    public:
        explicit CEP(std::vector<uint32_t> duration_limits);
        ~CEP();
        CEP(CEP&&);
        CEP& operator=(CEP&&);

        std::vector<std::vector<uint64_t>> denseFind(int n, const uint64_t * ts, std::vector<uint8_t * > conditions );
        std::vector<std::vector<uint64_t>> sparseFind(const uint64_t * ts, std::vector<std::vector<uint64_t>> conditions);

        // this function will update state as well as return numpy array right away
        py::array_t<uint64_t> do_arrow_batch(std::vector<uintptr_t> arrowArrayPtrs, std::vector<uintptr_t> arrowSchemaPtrs);
    
    private:
        std::vector<uint32_t> duration_limits_;
        uint32_t k_; // k is the number of conditions you got
        std::vector<std::vector<uint64_t>> findTuples(std::vector<std::vector<uint64_t>> lists);
};
    