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
# Init
###############################################################################

cd src
clean
# Fireup default test env
python3 admin.py&
if [[ $? -ne 0 ]]; then
  echo "Failed to fireup the test env. Please debug separtely!"
  exit 1
fi
sleep 3

# Create file with test msg
TEST_MSG="TEST MESSAGE"
NOT_FOUND_MSG="[Client] File Not Found"
echo $TEST_MSG > tmp.txt

###############################################################################
# Put
###############################################################################
python3 client.py put tmp.txt tmp
if [[ $? -ne 0 ]]; then
  echo "Put operation failed!"
  clean
  exit 1
fi

###############################################################################
# Get
###############################################################################
output=$(python3 client.py get tmp)
echo $output
if [[ $output != "[Client] Get from tmp: $TEST_MSG" ]]; then
  echo "GET operation failed!"
  clean
  exit 1
fi

###############################################################################
# Delete
###############################################################################
python3 client.py delete tmp

# Try Get
output=$(python3 client.py get tmp)
echo $output
if [[ $output != "$NOT_FOUND_MSG" ]]; then
  echo "Delete operation failed!"
  clean
  exit 1
fi

###############################################################################
# Clean up and quit
###############################################################################
echo "Basic Functional Test Complete!"

clean
sleep 3
echo "Unit test"

###############################################################################
# Unit test
###############################################################################
cd test
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

exit 0

