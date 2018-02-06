How to run
==========

> cd tests/
> python2 manual.py -f test_files/comma_delim # For testsing on specific files

Running the tests for comparison :

> for i in `seq 1 1 10`; do echo "========== $i ========"; python2 tests.py; done &> readlines.txt

How to do profiling:

> python2 -m cProfile -s cumtime -o readlines.log manual.py
> pyprof2calltree -k -i readlines.log

Profiling info
==============

Some very simple profiling info is in the tests folder with the original(.txt) and the readlines(.txt) showing the improvements.

