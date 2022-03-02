
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++14 -Wall -g

all: correctness persistence

test: sstable.o kvstore.o test.o

correctness: sstable.o kvstore.o correctness.o

persistence: sstable.o kvstore.o persistence.o

clean:
	-rm -f correctness persistence test *.o
	-rm -rf ./data/*