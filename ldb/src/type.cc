#include "type.h"

BufferView* BufferView::bufferViewFromType(const Type * type, const void* ptr, int64_t length) {
    if(dynamic_cast<const UInt8*>(type)){
        return new UInt8BufferView(ptr, length);
    }
    else if (dynamic_cast<const UInt16* >(type))
    {
        return new UInt16BufferView(ptr, length);
    } else if (dynamic_cast<const UInt32* >(type))
    {
        return new UInt32BufferView(ptr, length);
    } else if (dynamic_cast<const UInt64* >(type))
    {
        return new UInt64BufferView(ptr, length);
    } else if (dynamic_cast<const Int8* >(type))
    {
        return new Int8BufferView(ptr, length);
    } else if (dynamic_cast<const Int16* >(type))
    {
        return new Int16BufferView(ptr, length);
    } else if (dynamic_cast<const Int32* >(type))
    {
        return new Int32BufferView(ptr, length);
    } else if (dynamic_cast<const Int64* >(type))
    {
        return new Int64BufferView(ptr, length);
    } else if (dynamic_cast<const Half* >(type))
    {
        return new HalfBufferView(ptr, length);
    } else if (dynamic_cast<const Float* >(type))
    {
        return new FloatBufferView(ptr, length);
    } else if (dynamic_cast<const Double* >(type))
    {
        return new DoubleBufferView(ptr, length);
    } else {
        return nullptr;
    }
}