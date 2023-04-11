#include "tdigest.h"
#include "random"
#include "iostream"
#include "chrono"
#include "vector"

int main( int argc, char *argv[]) {
    TDigest td(100, atoi(argv[1]));
    // generate a vector of random floats
    std::vector<double> v;
    for (int i = 0; i < atoi(argv[2]); ++i) {
        // generate a random float
        double a = ((double)rand() / (RAND_MAX));
        v.push_back(a);
    }
    auto start_time = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < atoi(argv[2]); ++i) {
        // generate a random float
        td.Add(v[i]);
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = end_time - start_time;
    std::cout << "Time taken: " << diff.count() << " s" << std::endl;
    return 0;
}
