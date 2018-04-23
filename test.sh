#!/bin/bash
# Author: Diyi Wang (wang.di@husky.neu.edu)
#
# Functional test script for PyDFS project.

# Clean up 
clean(){
  pkill -f admin.py
  rm -f tmp.txt
  rm -f fs.img
}

###############################################################################
# Unit test
###############################################################################
cd src/test
clean
# Here all the unittest suite should be called from command line, since the test
# cases inside may have implicit order. Run `python -m unittest` will run those
# test cases in wrong order.
python3 test_master.py
if [[ $? -ne 0 ]]; then
  echo "Mater is not correct"
  clean
  exit 1
fi
python3 test_minion.py
if [[ $? -ne 0 ]]; then
  echo "Minion is not correct"
  clean
  exit 1
fi
python3 test_proxy.py
if [[ $? -ne 0 ]]; then
  echo "Proxy is not correct"
  clean
  exit 1
fi

###############################################################################
# Functional test
###############################################################################
python3 test_functional.py
if [[ $? -ne 0 ]]; then
  echo "System is not fully functional, plz check the error message"
  clean
  exit 1
fi


exit 0

