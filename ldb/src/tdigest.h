// approximate quantiles from arbitrary length dataset with O(1) space
// based on 'Computing Extremely Accurate Quantiles Using t-Digests' from Dunning & Ertl
// - https://arxiv.org/abs/1902.04023
// - https://github.com/tdunning/t-digest

#pragma once

#include <cmath>
#include <memory>
#include <vector>
#include <assert.h>
#include "bridge.h"

#define PREDICT_FALSE(x) (__builtin_expect(!!(x), 0))

class TDigest {
    public:
        explicit TDigest(uint32_t delta = 100, uint32_t buffer_size = 500);
        ~TDigest();
        TDigest(TDigest&&);
        TDigest& operator=(TDigest&&);

        // reset and re-use this tdigest
        void Reset();

        // dump internal data, only for debug
        void Dump() const;

        // buffer a single data point, consume internal buffer if full
        // this function is intensively called and performance critical
        // call it only if you are sure no NAN exists in input data
        void Add(double value) {
            assert(!std::isnan(value));
            if (PREDICT_FALSE(input_.size() == input_.capacity())) {
            MergeInput();
            }
            input_.push_back(value);
        }

        // skip NAN on adding
        template <typename T>
        typename std::enable_if<std::is_floating_point<T>::value>::type NanAdd(T value) {
            if (!std::isnan(value)) Add(value);
        }

        template <typename T>
        typename std::enable_if<std::is_integral<T>::value>::type NanAdd(T value) {
            Add(static_cast<double>(value));
        }

        // merge with other t-digests, called infrequently
        void Merge(const std::vector<TDigest>& others);
        void Merge(const TDigest& other);

        // calculate quantile
        double Quantile(double q) const;

        double Min() const { return Quantile(0); }
        double Max() const { return Quantile(1); }
        double Mean() const;

        // check if this tdigest contains no valid data points
        bool is_empty() const;

        void add_arrow(uintptr_t arrowArrayPtr, uintptr_t arrowSchemaPtr);

    private:
        // merge input data with current tdigest
        void MergeInput() const;

        // input buffer, size = buffer_size * sizeof(double)
        mutable std::vector<double> input_;

        // hide other members with pimpl
        class TDigestImpl;
        std::unique_ptr<TDigestImpl> impl_;
};


class NTDigest {
    public:
        explicit NTDigest(uint32_t n = 1, uint32_t delta = 100, uint32_t buffer_size = 500);
        ~NTDigest();
        NTDigest(NTDigest&&);
        NTDigest& operator=(NTDigest&&);
        void add_arrow(uint32_t k, uintptr_t arrowArrayPtr, uintptr_t arrowSchemaPtr);
        double Quantile(uint32_t k, double q) const;
        void batch_add_arrow(std::vector<uintptr_t> arrowArrayPtrs, std::vector<uintptr_t> arrowSchemaPtrs);

    private:
        std::vector<TDigest> tdigests_;

};