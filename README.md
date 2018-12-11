# nested_dict_fs

Permanent Nested Dict via File System's Folders

This package allows storing permanent data on the drive via a hierarchical/nested dict-like API.
The data will be stored as nested directories structure over the file system.

# Usage
```python
from nested_dict_fs import NestedDictFS

k = NestedDictFS('/tmp/test', mode='c')
for i in range(3):
    k['a', i] = i
    k['b', i] = i

print(list(k.keys()))
# ['a', 'b']
 
print(list(k['a'].items()))
# [('1', 1), ('2', 2), ('3', 3)]
```

# Install (beta)
`python setup.py develop --user`

# License
[GPL](LICENSE)