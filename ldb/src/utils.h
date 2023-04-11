#include <stdint.h>
#include "exceptions.h"
#include <cassert>

// mostly just arrow utils that I need
namespace utils {

    constexpr int64_t CeilDiv(int64_t value, int64_t divisor) {
        return (value == 0) ? 0 : 1 + (value - 1) / divisor;
    }

    constexpr bool IsPowerOf2(int64_t value) {
        return value > 0 && (value & (value - 1)) == 0;
    }

    constexpr bool IsPowerOf2(uint64_t value) {
        return value > 0 && (value & (value - 1)) == 0;
    }

    constexpr int64_t RoundUpToPowerOf2(int64_t value, int64_t factor) {
        return (value + (factor - 1)) & ~(factor - 1);
    }

    constexpr uint64_t RoundUpToPowerOf2(uint64_t value, uint64_t factor) {
        return (value + (factor - 1)) & ~(factor - 1);
    }

    struct BitmapWordAlignParams {
        int64_t leading_bits;
        int64_t trailing_bits;
        int64_t trailing_bit_offset;
        const uint8_t* aligned_start;
        int64_t aligned_bits;
        int64_t aligned_words;
    };

    // Compute parameters for accessing a bitmap using aligned word instructions.
    // The returned parameters describe:
    // - a leading area of size `leading_bits` before the aligned words
    // - a word-aligned area of size `aligned_bits`
    // - a trailing area of size `trailing_bits` after the aligned words
    template <uint64_t ALIGN_IN_BYTES>
    inline BitmapWordAlignParams BitmapWordAlign(const uint8_t* data, int64_t bit_offset,
                                                int64_t length) {
        static_assert(IsPowerOf2(ALIGN_IN_BYTES),
                        "ALIGN_IN_BYTES should be a positive power of two");
        constexpr uint64_t ALIGN_IN_BITS = ALIGN_IN_BYTES * 8;

        BitmapWordAlignParams p;

        // Compute a "bit address" that we can align up to ALIGN_IN_BITS.
        // We don't care about losing the upper bits since we are only interested in the
        // difference between both addresses.
        const uint64_t bit_addr =
            reinterpret_cast<size_t>(data) * 8 + static_cast<uint64_t>(bit_offset);
        const uint64_t aligned_bit_addr = RoundUpToPowerOf2(bit_addr, ALIGN_IN_BITS);

        p.leading_bits = std::min<int64_t>(length, aligned_bit_addr - bit_addr);
        p.aligned_words = (length - p.leading_bits) / ALIGN_IN_BITS;
        p.aligned_bits = p.aligned_words * ALIGN_IN_BITS;
        p.trailing_bits = length - p.leading_bits - p.aligned_bits;
        p.trailing_bit_offset = bit_offset + p.leading_bits + p.aligned_bits;

        p.aligned_start = data + (bit_offset + p.leading_bits) / 8;
        return p;
    }

    static inline int CountLeadingZeros(uint64_t value) {
    int bitpos = 0;
    while (value != 0) {
        value >>= 1;
        ++bitpos;
    }
    return 64 - bitpos;
    }

    // Returns the minimum number of bits needed to represent an unsigned value
    static inline int NumRequiredBits(uint64_t x) { return 64 - CountLeadingZeros(x); }

    // Returns ceil(log2(x)).
    static inline int Log2(uint64_t x) {
        return NumRequiredBits(x - 1);
    }

    static constexpr bool GetBit(const uint8_t* bits, uint64_t i) {
        return (bits[i >> 3] >> (i & 0x07)) & 1;
    }
    static constexpr uint8_t kBitmask[] = {1, 2, 4, 8, 16, 32, 64, 128};
    static inline void SetBit(uint8_t* bits, int64_t i) { bits[i / 8] |= kBitmask[i % 8]; }

    static inline uint64_t PopCount(uint64_t bitmap) { return __builtin_popcountll(bitmap); }
    static inline uint32_t PopCount(uint32_t bitmap) { return __builtin_popcount(bitmap); }
    // Returns 'value' rounded down to the nearest multiple of 'factor'
    constexpr int64_t RoundDown(int64_t value, int64_t factor) {
        return (value / factor) * factor;
    }

    int64_t CountSetBits(const uint8_t* data, int64_t bit_offset, int64_t length) {
        constexpr int64_t pop_len = sizeof(uint64_t) * 8;
        assert(bit_offset > 0);
        int64_t count = 0;

        const auto p = BitmapWordAlign<pop_len / 8>(data, bit_offset, length);
        for (int64_t i = bit_offset; i < bit_offset + p.leading_bits; ++i) {
            if (GetBit(data, i)) {
            ++count;
            }
        }

        if (p.aligned_words > 0) {
            // popcount as much as possible with the widest possible count
            const uint64_t* u64_data = reinterpret_cast<const uint64_t*>(p.aligned_start);
            assert(reinterpret_cast<size_t>(u64_data) & 7 == 0);
            const uint64_t* end = u64_data + p.aligned_words;

            constexpr int64_t kCountUnrollFactor = 4;
            const int64_t words_rounded = RoundDown(p.aligned_words, kCountUnrollFactor);
            int64_t count_unroll[kCountUnrollFactor] = {0};

            // Unroll the loop for better performance
            for (int64_t i = 0; i < words_rounded; i += kCountUnrollFactor) {
            for (int64_t k = 0; k < kCountUnrollFactor; k++) {
                count_unroll[k] += PopCount(u64_data[k]);
            }
            u64_data += kCountUnrollFactor;
            }
            for (int64_t k = 0; k < kCountUnrollFactor; k++) {
            count += count_unroll[k];
            }

            // The trailing part
            for (; u64_data < end; ++u64_data) {
            count += PopCount(*u64_data);
            }
        }

        // Account for left over bits (in theory we could fall back to smaller
        // versions of popcount but the code complexity is likely not worth it)
        for (int64_t i = p.trailing_bit_offset; i < bit_offset + length; ++i) {
            if (GetBit(data, i)) {
            ++count;
            }
        }

        return count;
    }

}
