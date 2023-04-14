CXX = g++
CXXFLAGS_SHARED= -g -DAVX2=1 -fopenmp -Wall -shared -std=c++17 -fPIC `python3 -m pybind11 --includes`
CXXFLAGS_EXE=-O2 -g -DAVX2=1 -fopenmp -Wall -std=c++17 `python3 -m pybind11 --includes`

LDFLAGS=-L.

LIBNAME=ldb.so
LIBSRC=ldb.cc cep.cc tdigest.cc bridge.cc type.cc
LIBOBJ=$(LIBSRC:.cc=.o)

BENCHSRC=tdigest_benchmark.cc tdigest.cc bridge.cc type.cc
BENCHOBJ=$(BENCHSRC:.cc=.o)
BENCHEXE=benchmark

all: $(LIBNAME) $(BENCHEXE)

$(LIBNAME): $(LIBOBJ)
	$(CXX) $(CXXFLAGS_SHARED) $(LDFLAGS) -o $(LIBNAME) $(LIBOBJ)

$(BENCHEXE): $(BENCHSRC)
	$(CXX) $(CXXFLAGS_EXE) $(LDFLAGS) $(BENCHSRC) -o $(BENCHEXE) 

%.o: %.cc
	$(CXX) $(CXXFLAGS_SHARED) $(CXXFLAGS) -c -o $@ $<

clean:
	rm -f $(LIBNAME) $(LIBOBJ) $(BENCHOBJ) $(BENCHEXE)
