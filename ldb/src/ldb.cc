#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>
#include "tdigest.h"
// #include "bloom_filter.h"

namespace py = pybind11;

PYBIND11_MODULE(ldb, m) {
    py::class_<TDigest>(m, "TDigest")
        .def(py::init<uint32_t, uint32_t>())
        .def("add", &TDigest::Add)
        .def("add_arrow", &TDigest::add_arrow)
        .def("quantile", &TDigest::Quantile);
    py::class_<NTDigest>(m, "NTDigest")
        .def(py::init<int32_t, uint32_t, uint32_t>())
        .def("add_arrow", &NTDigest::add_arrow)
        .def("batch_add_arrow", &NTDigest::batch_add_arrow)
        .def("quantile", &NTDigest::Quantile);
    /*py::class_<BlockedBloomFilter>(m, "BlockedBloomFilter")
        .def(py::init<uint64_t>())
        .def("insert", &BlockedBloomFilter::Insert)
        .def("find_arrow", &BlockedBloomFilter::find_arrow);*/
}
