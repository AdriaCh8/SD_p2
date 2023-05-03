# Sharded Key-value Storage System
###  Distributed Systems course: Assignment 2
###  Universitat Rovira i Virgili

# Installation
```bash
sudo chmod u+x setup.sh
pip3 install -e .
```

# Evaluation
## First subtask (simple KV storage)
```bash
python3 eval/single_node_storage.py
```

## Second subtask (sharded KV storage)
```bash
python3 eval/sharded.py
```

## Third subtask (sharded KV storage with replica groups)
```bash
python3 eval/replicas.py
```
