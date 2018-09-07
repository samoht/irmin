.PHONY: all clean test

all:
	dune build

test:
	dune runtest

examples:
	dune build @examples

clean:
	dune clean
