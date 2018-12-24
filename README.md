# nesteddict

Permanent nested dict via file system's folders.

This package allows storing permanent data on the drive via a hierarchical/nested dict-like API.
The data will be stored as nested directories structure over the file system.


# Features
- Dict like API for permanent data storage.
- Hierarchical structure.
- Traversing/searching items in the nested tree.
- Multiple storage engine.
- Compression.
- Smart caching of items.


# Install (beta)
`python setup.py develop --user`


# Basic Usage
NestedDictFS supports dict-like API: `k[...]`, `k.get(...)`, `k.put(...)`, `k.keys()`, etc...
However, tuple keys is considered as nested items: `k['a', 'b']` is equivalent to `k['a']['b']`.

In addition to the `get()` method, NestedDictFS also supports `get_child()` and `get_data()` which returns only
child items and data items respectively.
`get_child()` will create a child (lazy) if it does not exit.

```python
from nesteddict import NestedDictFS

# Creating nested dict obj. mode='c' will create the folder (lazy) if it doesn't exist.
k = NestedDictFS('/tmp/test', mode='c')
for i in range(3):
    k['x', i] = i
    k['y', i] = i

print(list(k.keys()))
# ['x', 'y']
 
print(list(k['x'].items()))
# [('1', 1), ('2', 2), ('3', 3)]

print(k['x', 2])
# 2

print(k['x'])
# NestedDictFS('/tmp/test/x', 'c')

print(k.get('z', 'default-value'))
# default-value

u = k.get_child('u')
print(u)
# NestedDictFS('/tmp/test/u', 'c')

u['t'] = 5
print(u['t'], k['u', 't'])
# 5 5

k.put('z', 'abc')
print(k['z'])
# abc

k.append('z', 'efg')
print(k['z'])
# ['abc', 'efg']
```

# Traversing Tree
NestedDictFS supports traversing an entire tree.

```python
from nesteddict import NestedDictFS
k = NestedDictFS('/tmp/test', mode='c')
for i in range(3):
    k['x', i] = i
    k['y', i] = i

print(list(k.walk(yield_keys=True, yield_values=False)))
# [(), 'y', 'x', ('y', '1'), ('y', '2'), ('y', '0'), ('x', '1'), ('x', '2'), ('x', '0')]

print(list(k.walk(yield_keys=False, yield_values=True)))
# [NestedDictFS(/tmp/test, c), NestedDictFS(/tmp/test/y, c), NestedDictFS(/tmp/test/x, c), 1, 2, 0, 1, 2, 0]

print(list(k.walk(include_child=False, include_data=True, yield_keys=True, yield_values=True)))
# [(('y', '1'), 1), (('y', '2'), 2), (('y', '0'), 0), (('x', '1'), 1), (('x', '2'), 2), (('x', '0'), 0)]
```


# Search
NestedDictFS supports search query using slice (`:`) and ellipsis (`...`).
Slice includes all the child keys and ellipsis includes all the sub-tree.

The following search type are supported:
- `NestedDictFS.items[query]`: iterate over key/value pairs that their key matches the query.
- `NestedDictFS.keys[query]`: iterate over keys that match the query.
- `NestedDictFS.values[query]`: iterate over values that their key matches the query.

In addition, using the prefix `child_` (e.g., `NestedDictFS.child_keys`) will only yield child items,
and using the prefix `data_` will only yield data items. 

For all of the query types above: `NestedDictFS.<query_type>()` is equivalent to `NestedDictFS.<query_type>[:]`.
Similarly, `NestedDictFS.walk()` is equivalent to `NestedDictFS.items[...]`.

```python
from nesteddict import NestedDictFS
k = NestedDictFS('/tmp/test', mode='c')
for i in range(1, 4):
    k['x', i] = i
    k['y', i] = i
    k['x', 'y', i] = i

print(list(k.keys['x', :]))
# [('x', '1'), ('x', '2'), ('x', '3'), ('x', 'y')]

print(list(k.items['x', :]))
# [(('x', '1'), 1), (('x', '2'), 2), (('x', '3'), 3), (('x', 'y'), NestedDictFS(/tmp/test/x/y, c))]

print(list(k.data_keys['x', :]))
# [('x', '1'), ('x', '2'), ('x', '3')]

print(list(k.child_keys['x', :]))
# [('x', 'y')]

print(list(k.child_values[:]))
# [NestedDictFS(/tmp/test/x, c), NestedDictFS(/tmp/test/y, c)]

print(list(k.keys[:, 1]))
# [('x', '1'), ('y', '1')]

print(list(k.keys[..., 'y']))
# [('y', '1'), ('y', '2'), ('y', '3'), ('x', 'y', '1'), ('x', 'y', '2'), ('x', 'y', '3')]

print(list(k.keys['x', ..., :]))
# [('x', '1'), ('x', 'y', '1')]
```


# Storage Engine
NestedDictFS currently supports storing the data in the following formats via the `store_engine` init argument:
- plain: convert any object plain text (utf-8).
- binary: attempt to write any object directly to a file.
- pickle: python default pickle implementation.
- msgpack: uses msgpack to store objects.
- msgpack-numpy (default): uses msgpack with msgpack_numpy to also support numpy arrays.

NestedDictFS optionally uses `gzip` to compress the stored data.
The init argument `compress_level` can be set from 0 to 9 (see `gzip` documentation).

```python
from nesteddict import NestedDictFS
k = NestedDictFS('/tmp/test-msgpack', mode='c', store_engine='msgpack')
k['a'] = {'b': 1}

print(k['a'])
# {'b': 1}

k.append('a', 5)
print(k['a'])
# [{'b': 1}, 5]

k = NestedDictFS('/tmp/test-numpy', mode='c', store_engine='msgpack-numpy')
import numpy as np
k['a'] = np.array([1,2,3], dtype=np.uint32)

print(k['a'])
# array([1, 2, 3], dtype=uint32)

k.append('a', 5)
print(k['a'])
# [array([1, 2, 3], dtype=uint32), 5]

k = NestedDictFS('/tmp/test-plain', mode='c', store_engine='plain', compress_level=0)
k['a'] = {'b': 1}

print(k['a'])
# {'b': 1}

k.append('a', 5)
print(k['a'])
# {'b': 1}5

with open('/tmp/test-plain/a', 'r') as f:
    print(f.read())
# {'b': 1}5
```

# License
[GPL](LICENSE.txt)