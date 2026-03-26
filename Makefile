.PHONY: all clean server client build

all: build

build:
	cmake -B build
	cmake --build build -j$$(nproc)

server: build
	./bin/flexql-server

client: build
	./bin/flexql-client

clean:
	rm -rf build bin
	rm -f data/wal/server.log
