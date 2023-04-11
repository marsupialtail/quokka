#include <stdint.h>
#include "exceptions.h"

struct Type {
    virtual ~Type() = default;
    virtual int64_t size() const = 0;
    virtual bool is_primitive() const = 0;
};
struct Boolean : Type {
    int64_t size() const override { return 1; }
    bool is_primitive() const override { return true; }
};
struct Int8 : Type {
    int64_t size() const override { return 8; }
    bool is_primitive() const override { return true; }
};
struct Int16 : Type {
    int64_t size() const override { return 16; }
    bool is_primitive() const override { return true; }
};
struct Int32 : Type {
    int64_t size() const override { return 32; }
    bool is_primitive() const override { return true; }
};
struct Int64 : Type {
    int64_t size() const override { return 64; }
    bool is_primitive() const override { return true; }
};
struct UInt8 : Type {
    int64_t size() const override { return 8; }
    bool is_primitive() const override { return true; }
};
struct UInt16 : Type {
    int64_t size() const override { return 16; }
    bool is_primitive() const override { return true; }
};
struct UInt32 : Type {
    int64_t size() const override { return 32; }
    bool is_primitive() const override { return true; }
};
struct UInt64 : Type {
    int64_t size() const override { return 64; }
    bool is_primitive() const override { return true; }
};
struct Half : Type {
    int64_t size() const override { return 16; }
    bool is_primitive() const override { return true; }
};
struct Float : Type {
    int64_t size() const override { return 32; }
    bool is_primitive() const override { return true; }
};
struct Double : Type {
    int64_t size() const override { return 64; }
    bool is_primitive() const override { return true; }
};


class BufferView {
public:
    BufferView(const void* ptr, int64_t length, int64_t stride) : ptr_(ptr), length_(length), stride_(stride) {}
    static BufferView* bufferViewFromType(const Type * type, const void* ptr, int64_t length);
    const void* ptr() const { return ptr_; }
    const virtual double valueDouble(int64_t index) const = 0;
    int64_t length() const { return length_; }
    int64_t stride() const { return stride_; }
    ~BufferView() = default;
private:
    const void* ptr_;
    int64_t length_;
    int64_t stride_;
};

class Int8BufferView : public BufferView {
public:
    Int8BufferView(const void* ptr, int64_t length) : BufferView(ptr, length, 8) {}
    const int8_t* ptr() const { return static_cast<const int8_t*>(BufferView::ptr()); }
    const double valueDouble(int64_t index) const { return (double) ptr()[index]; }
};

class Int16BufferView : public BufferView {
public:
    Int16BufferView(const void* ptr, int64_t length) : BufferView(ptr, length, 16) {}
    const int16_t* ptr() const { return static_cast<const int16_t*>(BufferView::ptr()); }
    const double valueDouble(int64_t index) const { return (double) ptr()[index]; }
};

class Int32BufferView : public BufferView {
public:
    Int32BufferView(const void* ptr, int64_t length) : BufferView(ptr, length, 32) {}
    const int32_t* ptr() const { return static_cast<const int32_t*>(BufferView::ptr()); }
    const double valueDouble(int64_t index) const { return (double) ptr()[index]; }
};  

class Int64BufferView : public BufferView {
public:
    Int64BufferView(const void* ptr, int64_t length) : BufferView(ptr, length, 64) {}
    const int64_t* ptr() const { return static_cast<const int64_t*>(BufferView::ptr()); }
    const double valueDouble(int64_t index) const { return (double) ptr()[index]; }
};

class UInt8BufferView : public BufferView {
public:
    UInt8BufferView(const void* ptr, int64_t length) : BufferView(ptr, length, 8) {}
    const uint8_t* ptr() const { return static_cast<const uint8_t*>(BufferView::ptr()); }
    const double valueDouble(int64_t index) const { return (double) ptr()[index]; }
};

class UInt16BufferView : public BufferView {
public:
    UInt16BufferView(const void* ptr, int64_t length) : BufferView(ptr, length, 16) {}
    const uint16_t* ptr() const { return static_cast<const uint16_t*>(BufferView::ptr()); }
    const double valueDouble(int64_t index) const { return (double) ptr()[index]; }
};

class UInt32BufferView : public BufferView {
public:
    UInt32BufferView(const void* ptr, int64_t length) : BufferView(ptr, length, 32) {}
    const uint32_t* ptr() const { return static_cast<const uint32_t*>(BufferView::ptr()); }
    const double valueDouble(int64_t index) const { return (double) ptr()[index]; }
};

class UInt64BufferView : public BufferView {
public:
    UInt64BufferView(const void* ptr, int64_t length) : BufferView(ptr, length, 64) {}
    const uint64_t* ptr() const { return static_cast<const uint64_t*>(BufferView::ptr()); }
    const double valueDouble(int64_t index) const { return (double) ptr()[index]; }
};

class HalfBufferView : public BufferView {
public:
    HalfBufferView(const void* ptr, int64_t length) : BufferView(ptr, length, 16) {}
    const uint16_t* ptr() const { return static_cast<const uint16_t*>(BufferView::ptr()); }
    const double valueDouble(int64_t index) const { return (double) ptr()[index]; }
};

class FloatBufferView : public BufferView {
public:
    FloatBufferView(const void* ptr, int64_t length) : BufferView(ptr, length, 32) {}
    const float* ptr() const { return static_cast<const float*>(BufferView::ptr()); }
    const double valueDouble(int64_t index) const { return (double) ptr()[index]; }
};

class DoubleBufferView : public BufferView {
public:
    DoubleBufferView(const void* ptr, int64_t length) : BufferView(ptr, length, 64) {}
    const double* ptr() const { return static_cast<const double*>(BufferView::ptr()); }
    const double valueDouble(int64_t index) const { return ptr()[index]; }
};
