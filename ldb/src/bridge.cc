#include "bridge.h"

Type * importFromArrow(const ArrowSchema * arrowSchema) {
  const char* format = arrowSchema->format;
  ASSERT_MSG(format != nullptr, "format must not be null");

  switch (format[0]) {
    case 'b':
        return new Boolean();
    case 'c':
        return new Int8();
    case 's':
        return new Int16();
    case 'i':
        return new Int32();
    case 'l':
        return new Int64();
    case 'C':
        return new UInt8();
    case 'S':
        return new UInt16();
    case 'I':
        return new UInt32();
    case 'L':   
        return new UInt64();
    case 'e':
        return new Half();
    case 'f':
        return new Float();
    case 'g':
        return new Double();

  }
}

BufferView * bufferFromArrow(uintptr_t arrowArrayPtr, uintptr_t arrowSchemaPtr) {
    auto arrowArray = reinterpret_cast<ArrowArray*>(arrowArrayPtr);
    auto arrowSchema = reinterpret_cast<ArrowSchema*>(arrowSchemaPtr);
    ASSERT_MSG(arrowSchema->release != nullptr, "arrowSchema was released.");
    ASSERT_MSG(arrowArray->release != nullptr, "arrowArray was released.");
    ASSERT_MSG(arrowArray->offset == 0, "Offsets are not supported during arrow conversion yet.");
    ASSERT_MSG(arrowArray->length > 0, "Array length needs to be non-negative.");

    Type * type = importFromArrow(arrowSchema);
    ASSERT_MSG(arrowArray->null_count == 0, "ldb kernels cannot handle nulls yet.")

    // Wrap the values buffer into a Velox BufferView - zero-copy.
    ASSERT_MSG(arrowArray->n_buffers == 2, "Primitive types expect two buffers as input.");
    return BufferView::bufferViewFromType(type, arrowArray->buffers[1], arrowArray->length);
}