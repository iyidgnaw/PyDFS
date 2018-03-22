#!/bin/bash
# Author: Diyi Wang (wang.di@husky.neu.edu)
#
# Functional test script for PyDFS project.

# Clean up 
clean(){
  pkill -f master.py
  pkill -f minion.py
  rm -f tmp.txt
  rm -f fs.img
}
###############################################################################
# Init
###############################################################################

cd pydfs
clean
# Bring up master and minions
python3 master.py &
if [[ $? -ne 0 ]]; then
  echo "Master fireup failed!"
  exit 1
fi

python3 minion.py 8888&
python3 minion.py 8889&
python3 minion.py 8890&
if [[ $? -ne 0 ]]; then
  echo "Minions fireup failed!"
  clean
  exit 1
fi

# Wait for server and minion
sleep 1

# Create file with test msg
TEST_MSG="Put and Get are all green!"
NOT_FOUND_MSG="404: file not found"
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
if [[ $output != $TEST_MSG  ]]; then
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
if [[ $output != $NOT_FOUND_MSG ]]; then
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

