# camus

[![Build Status](https://travis-ci.org/gyounes/camus.svg?branch=master)](https://travis-ci.org/gyounes/camus/)

CAMUS stands for CAusal MUlticast Store, whic is an erlang library providing different causal multicast backends:
- Update: As camus will be used for minidote, the only backend in thios repo is the TCB (Tagged Causal Broadcast)


### More about TCB
- [END-TO-END ARGUMENTS IN SYSTEM DESIGN](https://web.mit.edu/Saltzer/www/publications/endtoend/endtoend.pdf), J.H. Saltzer, D.P. Reed and D.D. Clark (1984)
- [Understanding the Limitations of Causally and Totally Ordered Communication](https://www.cs.rice.edu/~alc/comp520/papers/Cheriton_Skeen.pdf), David R. Cheriton, Dale Skeen (1993)
- [The Pitfalls in Achieving Tagged Causal Delivery](https://haslab.uminho.pt/gry/files/papoc_18.pdf), Georges Younes, Paulo Sérgio Almeida and Carlos Baquero (2018)


Development
-----------

Use the following `Makefile` targets to build and test camus:

	# compile the project:
	make compile

	# run the unit tests and camus test suite:
	make test

	# Run dialyzer to check types:
	make dialyzer

	# Run tests, check xref, dialyzer and linting:
	make check

	# watch logs:
	make logs

	# Open a shell:
	make shell

	# Build a release:
	make rel
