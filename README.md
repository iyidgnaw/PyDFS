# PyDFS
Simple fault tolerant distributed file system like HDFS (and of-course GFS). It consists of one Master (NameNode) and multiple Minions (DataNode). And a client for interation. It will dump metadata/namespace when given SIGINT and reload it when fired up next time. Replicate data  the way HDFS does. It will send data to one minion and that minion will send it to next one and so on. Reading done in similar manner. Will contact fitst minion for block, if fails then second and so on.  Uses RPyC for RPC.

#### [Blog: Simple Distributed File System in Python : PyDFS](https://superuser.blog/distributed-file-system-python/)

### Requirements:
  - rpyc (Really! That's it.)
  
### How to run.
  0. pip3 install -r requirements.txt
  1. Edit `conf.py` for setting block size, replication factor and list minions (`minionid:host:port`)
  2. Fireup master and minions using `python admin.py`
  3. To store and retrieve a file:
```sh
$ python client.py put sourcefile.txt sometxt
$ python client.py get sometxt
```
##### Stop it using Ctll + C so that it will dump the namespace.

## TODO:
Priority:
  - ~~Minion heartbeats~~ / Block reports (Luke)
  - Add entry in namespace only after write succeeds
  - Admin interface: ~~NODE_CREATE, NODE_KILL, GET, PUT, DELETE~~ (Luke)
  - ~~Delete/Add minion node~~ (diyi)
  - ~~Block integrity check~~ (diyi)

Optional:
  - Use better algo for minion selection to put a block (currently random)
  - Dump namespace periodically (check-pointing)
  - Use proper datastructure(tree-like eg. treedict) to store namespace(currently simple dict)

### Dev
#### To use the pre commit hook
  1. copy pre-commit to .git/hook and change the file permission if necessary
  2. Install Pylint (for python3) and use "google-pylint.rc" as your pylint config

### Issue:
  1. ReferenceError in master sync.
