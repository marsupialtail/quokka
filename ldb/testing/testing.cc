#include "chrono"
#include "iostream"
#include "random"
#include "vector"
#include "algorithm"

int main(int argc, char *argv[]) {
  // generate a vector of random doubles
  std::vector<double> v;
  for (int i = 0; i < atoi(argv[1]); ++i) {
    // generate a random double
    double a = ((double)rand() / (RAND_MAX));
    v.push_back(a);
  }
  auto start_time = std::chrono::high_resolution_clock::now();
  // sort every 500 elements of this vector with std sort
    for (int i = 0; i < atoi(argv[1]); i += 500) {
        std::sort(v.begin() + i, v.begin() + i + 500);
    }   
  auto end_time = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> diff = end_time - start_time;
  std::cout << "Time taken: " << diff.count() << " s" << std::endl;
  return 0;
}