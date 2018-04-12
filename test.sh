#!/bin/bash
# Author: Diyi Wang (wang.di@husky.neu.edu)
#
# Functional test script for PyDFS project.

# Clean up 
clean(){
  pkill -f master.py
  pkill -f minion.py
  pkill -f proxy.py
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
echo "Test Complete!"

clean
exit 0

