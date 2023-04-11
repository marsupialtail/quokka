#include <pybind11/pybind11.h>


struct ArrowSchema {
  // Array type description
  const char* format;
  const char* name;
  const char* metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema** children;
  struct ArrowSchema* dictionary;

  // Release callback
  void (*release)(struct ArrowSchema*);
  // Opaque producer-specific data
  void* private_data;
};

struct ArrowArray {
  // Array data description
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void** buffers;
  struct ArrowArray** children;
  struct ArrowArray* dictionary;

  // Release callback
  void (*release)(struct ArrowArray*);
  // Opaque producer-specific data
  void* private_data;
};

m.def(
      "import_from_arrow",
      [](uintptr_t arrowArrayPtr, uintptr_t arrowSchemaPtr) {
        auto arrowArray = reinterpret_cast<ArrowArray*>(arrowArrayPtr);
        auto arrowSchema = reinterpret_cast<ArrowSchema*>(arrowSchemaPtr);
        std::shared_ptr<facebook::velox::memory::MemoryPool> pool_{facebook::velox::memory::getDefaultMemoryPool()};
        return importFromArrowAsOwner(*arrowSchema, *arrowArray, pool_.get());
      });

namespace py = pybind11;

PYBIND11_MODULE(tdigest, m) {
    py::class_<TDigest>(m, "TDigest")
        .def(py::init<uint32_t, uint32_t>())
        .def("add", &TDigest::Add)
        .def("add_arrow", &TDigest::AddArrowArray)
        .def("quantile", &TDigest::Quantile);
    // declare that arrow::Int64Array is an acceptable arugment to add_arrow because it is a subclass of arrow::Array
    py::implicitly_convertible<arrow::Int64Array, arrow::Array>();

}