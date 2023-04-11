#pragma once
#include <immintrin.h>
#include <atomic>
#include <cstdint>
#include <memory>
#include <vector>
#include <cstring>
#include "bridge.h"

#define ROTL64(x, n) (((x) << (n)) | ((x) >> ((-n) & 63)))

template <typename T>
inline std::enable_if_t<std::is_trivially_copyable_v<T>, T> SafeLoadAs(
    const uint8_t* unaligned) {
  std::remove_const_t<T> ret;
  std::memcpy(&ret, unaligned, sizeof(T));
  return ret;
}

struct BloomFilterMasks {
  // Generate all masks as a single bit vector. Each bit offset in this bit
  // vector corresponds to a single mask.
  // In each consecutive kBitsPerMask bits, there must be between
  // kMinBitsSet and kMaxBitsSet bits set.
  //
  BloomFilterMasks();

  inline uint64_t mask(int bit_offset) {
    return (SafeLoadAs<uint64_t>(masks_ + bit_offset / 8) >>
            (bit_offset % 8)) &
           kFullMask;
  }

  // Masks are 57 bits long because then they can be accessed at an
  // arbitrary bit offset using a single unaligned 64-bit load instruction.
  //
  static constexpr int kBitsPerMask = 57;
  static constexpr uint64_t kFullMask = (1ULL << kBitsPerMask) - 1;

  // Minimum and maximum number of bits set in each mask.
  // This constraint is enforced when generating the bit masks.
  // Values should be close to each other and chosen as to minimize a Bloom
  // filter false positives rate.
  //
  static constexpr int kMinBitsSet = 4;
  static constexpr int kMaxBitsSet = 5;

  // Number of generated masks.
  // Having more masks to choose will improve false positives rate of Bloom
  // filter but will also use more memory, which may lead to more CPU cache
  // misses.
  // The chosen value results in using only a few cache-lines for mask lookups,
  // while providing a good variety of available bit masks.
  //
  static constexpr int kLogNumMasks = 10;
  static constexpr int kNumMasks = 1 << kLogNumMasks;

  // Data of masks. Masks are stored in a single bit vector. Nth mask is
  // kBitsPerMask bits starting at bit offset N.
  //
  static constexpr int kTotalBytes = (kNumMasks + 64) / 8;
  uint8_t masks_[kTotalBytes];
};

// A variant of a blocked Bloom filter implementation.
// A Bloom filter is a data structure that provides approximate membership test
// functionality based only on the hash of the key. Membership test may return
// false positives but not false negatives. Approximation of the result allows
// in general case (for arbitrary data types of keys) to save on both memory and
// lookup cost compared to the accurate membership test.
// The accurate test may sometimes still be cheaper for a specific data types
// and inputs, e.g. integers from a small range.
//
// This blocked Bloom filter is optimized for use in hash joins, to achieve a
// good balance between the size of the filter, the cost of its building and
// querying and the rate of false positives.
//
class BlockedBloomFilter {
  friend class BloomFilterBuilder_SingleThreaded;

 public:
  BlockedBloomFilter(uint64_t num_rows_to_insert) : log_num_blocks_(0), num_blocks_(0), blocks_(nullptr) {CreateEmpty(num_rows_to_insert);}

  inline bool Find(uint64_t hash) const {
    uint64_t m = mask(hash);
    uint64_t b = blocks_[block_id(hash)];
    return (b & m) == m;
  }

  std::pair<uintptr_t, uintptr_t> find_arrow(uintptr_t arrowArrayPtr, uintptr_t arrowSchemaPtr)
  {
        BufferView * buffer = bufferFromArrow(arrowArrayPtr, arrowSchemaPtr);
        
  }

  void Find(int64_t num_rows, const uint32_t* hashes,
            uint8_t* result_bit_vector, bool enable_prefetch = true) const;
  void Find(int64_t num_rows, const uint64_t* hashes,
            uint8_t* result_bit_vector, bool enable_prefetch = true) const;

  int log_num_blocks() const { return log_num_blocks_; }

  int NumHashBitsUsed() const;

  bool IsSameAs(const BlockedBloomFilter* other) const;

  int64_t NumBitsSet() const;

  // Folding of a block Bloom filter after the initial version
  // has been built.
  //
  // One of the parameters for creation of Bloom filter is the number
  // of bits allocated for it. The more bits allocated, the lower the
  // probability of false positives. A good heuristic is to aim for
  // half of the bits set in the constructed Bloom filter. This should
  // result in a good trade off between size (and following cost of
  // memory accesses) and false positives rate.
  //
  // There might have been many duplicate keys in the input provided
  // to Bloom filter builder. In that case the resulting bit vector
  // would be more sparse then originally intended. It is possible to
  // easily correct that and cut in half the size of Bloom filter
  // after it has already been constructed. The process to do that is
  // approximately equal to OR-ing bits from upper and lower half (the
  // way we address these bits when inserting or querying a hash makes
  // such folding in half possible).
  //
  // We will keep folding as long as the fraction of bits set is less
  // than 1/4. The resulting bit vector density should be in the [1/4,
  // 1/2) range.
  //
  void Fold();

  inline void Insert(uint64_t hash) {
    uint64_t m = mask(hash);
    uint64_t& b = blocks_[block_id(hash)];
    b |= m;
  }

  void Insert( int64_t num_rows, const uint32_t* hashes);
  void Insert( int64_t num_rows, const uint64_t* hashes);

 private:
  int CreateEmpty(int64_t num_rows_to_insert);

  inline uint64_t mask(uint64_t hash) const {
    // The lowest bits of hash are used to pick mask index.
    //
    int mask_id = static_cast<int>(hash & (BloomFilterMasks::kNumMasks - 1));
    uint64_t result = masks_.mask(mask_id);

    // The next set of hash bits is used to pick the amount of bit
    // rotation of the mask.
    //
    int rotation = (hash >> BloomFilterMasks::kLogNumMasks) & 63;
    result = ROTL64(result, rotation);

    return result;
  }

  inline int64_t block_id(uint64_t hash) const {
    // The next set of hash bits following the bits used to select a
    // mask is used to pick block id (index of 64-bit word in a bit
    // vector).
    //
    return (hash >> (BloomFilterMasks::kLogNumMasks + 6)) & (num_blocks_ - 1);
  }

  template <typename T>
  inline void InsertImp(int64_t num_rows, const T* hashes);

  template <typename T>
  inline void FindImp(int64_t num_rows, const T* hashes, uint8_t* result_bit_vector,
                      bool enable_prefetch) const;

  void SingleFold(int num_folds);

  inline __m256i mask_avx2(__m256i hash) const;
  inline __m256i block_id_avx2(__m256i hash) const;
  int64_t Insert_avx2(int64_t num_rows, const uint32_t* hashes);
  int64_t Insert_avx2(int64_t num_rows, const uint64_t* hashes);
  template <typename T>
  int64_t InsertImp_avx2(int64_t num_rows, const T* hashes);
  int64_t Find_avx2(int64_t num_rows, const uint32_t* hashes,
                    uint8_t* result_bit_vector) const;
  int64_t Find_avx2(int64_t num_rows, const uint64_t* hashes,
                    uint8_t* result_bit_vector) const;
  template <typename T>
  int64_t FindImp_avx2(int64_t num_rows, const T* hashes,
                       uint8_t* result_bit_vector) const;

  bool UsePrefetch() const {
    return num_blocks_ * sizeof(uint64_t) > kPrefetchLimitBytes;
  }

  static constexpr int64_t kPrefetchLimitBytes = 256 * 1024;

  static BloomFilterMasks masks_;
  // Total number of bits used by block Bloom filter must be a power
  // of 2.
  //
  int log_num_blocks_;
  int64_t num_blocks_;

  // Buffer allocated to store an array of power of 2 64-bit blocks.
  //
  void * buf_;
  // Pointer to mutable data owned by Buffer
  //
  uint64_t* blocks_;
};