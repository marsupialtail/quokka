// let's first make an array of data

#include <x86intrin.h>
#include <iostream>

// void inplace_cumsum_int32(int32_t* data, size_t numitems) {
//     // use x86 intrinsics to put a static array of 16 integers into an avx512 register
//     __m512i step1 = _mm512_set_epi32(0,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14);
//     unsigned int mask1 = 65534;

//     __m512i step2 = _mm512_set_epi32(0,  0,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13);
//     unsigned int mask2 = 65532;

//     __m512i step3 = _mm512_set_epi32(0,  0,  0,  0,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11);
//     unsigned int mask3 = 65520;

//     __m512i step4 = _mm512_set_epi32(0,  0,  0,  0,  0,  0,  0,  0,  0,  1,  2,  3,  4,  5,  6,  7);
//     unsigned int mask4 = 65280;

//     __m512i scatter = _mm512_set_epi32(15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15);
    
//     __m512i v;
//     // __m512i carry = _mm512_set_epi32(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    
//     for (size_t i = 0;  i < numitems;  i += 16) {
//         v = _mm512_loadu_epi32(&data[i]);
        
//         v = _mm512_add_epi32(v, _mm512_maskz_permutexvar_epi32(mask1, step1, v));
//         v = _mm512_add_epi32(v, _mm512_maskz_permutexvar_epi32(mask2, step2, v));
//         v = _mm512_add_epi32(v, _mm512_maskz_permutexvar_epi32(mask3, step3, v));
//         v = _mm512_add_epi32(v, _mm512_maskz_permutexvar_epi32(mask4, step4, v));
//         // v += carry;
        
//         // carry = _mm512_permutexvar_epi32(scatter, v);
        
//         _mm512_storeu_epi32(&data[i], v);
//     }
// }

void inplace_cumsum_int32_avx2(int32_t* data, size_t numitems){
    for (size_t i = 0;  i < numitems;  i += 8) {
        __m256i x = _mm256_loadu_si256((__m256i*)(data + i));
        
        x = _mm256_add_epi32(x, _mm256_slli_si256(x, 4));
        x = _mm256_add_epi32(x, _mm256_slli_si256(x, 8));
        // broadcast the fourth position of the x to a new ymm register
        
        // x = _mm256_add_epi32(x, _mm256_srli_si256(x, 4));
        // x = _mm256_add_epi32(x, _mm256_srli_si256(x, 8));
        
        _mm256_storeu_si256((__m256i*)(data + i), x);
    }
    
}

int main()
{
    int32_t data[1600];
    for (int i = 0;  i < 1600;  i++) {
        data[i] = 1;
    }
    inplace_cumsum_int32_avx2(data, 1600);
    for (int i = 0;  i < 1600;  i++) {
        std::cout << data[i] << std::endl;
    }
}