#include "cep.h"
#include <vector>

#include <queue>

CEP::CEP(std::vector<uint32_t> duration_limits) : duration_limits_(duration_limits), k_(duration_limits.size() + 1) {}
CEP::~CEP() = default;
CEP::CEP(CEP&&) = default;
CEP& CEP::operator=(CEP&&) = default;

std::vector<std::vector<uint64_t>> CEP::findTuples(std::vector<std::vector<uint64_t>> lists) {
    std::vector<std::vector<uint64_t>> results;
    uint64_t limit = lists.size();
    std::queue<std::pair<std::vector<uint64_t>, std::pair<uint64_t, uint64_t>>> q;
    q.push(std::make_pair(std::vector<uint64_t>{lists[0][0]}, std::make_pair(0, lists[0][0])));
    while (!q.empty()) {
        std::vector<uint64_t> curr_tup = q.front().first;
        std::pair<uint64_t, uint64_t> curr_index = q.front().second;
        q.pop();
        uint64_t curr_list = curr_index.first;
        uint64_t curr_value = curr_index.second;
        std::vector<std::pair<uint64_t, uint64_t>> to_add;
        for (uint64_t i = 0; i < lists[curr_list + 1].size(); i++) {
            uint64_t value = lists[curr_list + 1][i];
            if (value >= curr_value && value <= curr_value + duration_limits_[curr_list]) {
                to_add.push_back(std::make_pair(lists[curr_list + 1][i], value));
            }
        }
        if (curr_list == limit - 2) {
            for (auto tup : to_add) {
                curr_tup.push_back(tup.first);
                results.push_back(curr_tup);
                curr_tup.pop_back();
            }
        } else {
            for (auto tup : to_add) {
                curr_tup.push_back(tup.first);
                q.push(std::make_pair(curr_tup, std::make_pair(curr_list + 1, tup.second)));
                curr_tup.pop_back();
            }
        }
    }
    return results;
}

std::vector<std::vector<uint64_t>> CEP::sparseFind(const uint64_t * ts, std::vector<std::vector<uint64_t>> conditions) {
    std::vector<std::vector<uint64_t>> ret(k_);
    for (uint32_t i = 0; i < k_; i++) {
        ret[i] = std::vector<uint64_t>();
        ASSERT_MSG(conditions[i].size() > 0, "condition is empty, should not happen")
    }
    
    std::vector<uint64_t> fingers(k_);
    // ts[conditions[fingers[i]]] must be bigger than or equal to ts[conditions[fingers[i - 1]]]]
    // fingers[i] gets bigger than conditions[i].size() when it reaches the end, quit loop

    uint64_t start_time = ts[conditions[0][0]];
    for(uint32_t i = 1; i < k_; i++) {
        // walk finger[i] forward enough such that ts[conditions[i][fingers[i]]] >= start_time
        while (fingers[i] < conditions[i].size() && ts[conditions[i][fingers[i]]] < start_time) {
            fingers[i]++;
        }
        if (fingers[i] == conditions[i].size()) {
            // we are done
            return ret;
        }
        start_time = ts[conditions[i][fingers[i]]];
    }

    std::vector<std::vector<uint64_t>> local_ret(k_);
    for (uint32_t i = 0; i < k_; i++) {
        local_ret[i] = std::vector<uint64_t>();
    }

    while (fingers[0] < conditions[0].size()) {

        for (uint32_t i = 1; i < k_; i++) {
            if (fingers[i] == conditions[i].size()) {
                return ret;
            }
        }

        for (uint32_t i = 0; i < k_; i++) {
            local_ret[i].clear();
        }
        local_ret[0].push_back(conditions[0][fingers[0]]);

        // compute the window now
        uint64_t start_time = ts[conditions[0][fingers[0]]];
        uint64_t end_time = start_time + duration_limits_[0];
        for (uint32_t i = 1; i < k_; i++) {
            // increment fingers[i] until ts[conditions[i][fingers[i]]] >= start_time
            while (ts[conditions[i][fingers[i]]] < start_time && fingers[i] < conditions[i].size()) {
                fingers[i] ++;
            }
            start_time = ts[conditions[i][fingers[i]]];
            uint64_t local_finger = fingers[i];
            while (ts[conditions[i][local_finger]] <= end_time && local_finger < conditions[i].size()) {
                local_ret[i].push_back(conditions[i][local_finger]);
                local_finger ++;
            }
            end_time = ts[conditions[i][local_finger - 1]] + duration_limits_[i];
        }

        std::vector<std::vector<uint64_t>> expanded_results = findTuples(local_ret);
        

        for (auto vec : expanded_results) {
            for (uint32_t i = 0; i < k_; i++) {
                ret[i].push_back(vec[i]);
            }
        }
        fingers[0] ++;

        //extend the local_ret into ret
    }    
}

py::array_t<uint64_t> CEP::do_arrow_batch(std::vector<uintptr_t> arrowArrayPtrs, std::vector<uintptr_t> arrowSchemaPtrs) {
    assert(arrowArrayPtrs.size() == arrowSchemaPtrs.size());
    // arrowArrayPtrs should contain the following arrays:
    // 1. time in uint64_t datatype, the number of milliseconds after the epoch
    // 2 - n arbitary number of uint64_t type.
    // We should check for those.

    uintptr_t arrowArrayPtr = arrowArrayPtrs[0];
    uintptr_t arrowSchemaPtr = arrowSchemaPtrs[0];
    auto arrowSchema = reinterpret_cast<ArrowSchema*>(arrowSchemaPtrs[0]);
    const char* format = arrowSchema->format;
    ASSERT_MSG(format != nullptr, "format must not be null");
    ASSERT_MSG(format[0] == 'L', "format must be uint64_t");
    UInt64BufferView * timeBuffer = dynamic_cast<UInt64BufferView *>(bufferFromArrow(arrowArrayPtr, arrowSchemaPtr));
    const uint64_t* ts = (const uint64_t *)timeBuffer->ptr();
    
    std::vector<std::vector<uint64_t>> conditions;
    for(uint32_t i = 1; i < arrowArrayPtrs.size(); i++) {
        uintptr_t arrowArrayPtr = arrowArrayPtrs[i];
        uintptr_t arrowSchemaPtr = arrowSchemaPtrs[i];
        UInt64BufferView * timeBuffer = dynamic_cast<UInt64BufferView *>(bufferFromArrow(arrowArrayPtr, arrowSchemaPtr));
        std::vector<uint64_t> vec;
        const uint64_t* ptr = (const uint64_t *)timeBuffer->ptr();
        for (uint32_t i = 0; i < timeBuffer->length(); i++) {
            vec.push_back(ptr[i]);
        }
        conditions.push_back(vec);
    }

    std::vector<std::vector<uint64_t>> results = CEP::sparseFind(ts, conditions);

    // turn the results into a py::array_t<uint32_t>

    auto ret = py::array_t<uint64_t>(py::array::ShapeContainer({ k_, results[0].size()}));
    py::buffer_info buf = ret.request();
    uint64_t* ptr = (uint64_t*) buf.ptr;
    for (uint32_t i = 0; i < k_; i++) {
        for (uint32_t j = 0; j < results[0].size(); j++) {
            ptr[i * results[0].size() + j] = results[i][j];
        }
    }
    return ret;
        
}