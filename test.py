"""
Author: Liran Funaro <funaro@cs.technion.ac.il>

Copyright (C) 2006-2018 Liran Funaro

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""
import os
import uuid
import unittest
import shutil
import tempfile
import itertools
import numpy as np

from nested_dict_fs import NestedDictFS, NDFSKeyError, NDFSLookupError, NDFSAccessViolation


def random_folder():
    return uuid.uuid4().hex.upper()[:8]


def clean(path):
    if os.path.isdir(path):
        shutil.rmtree(path)
    elif os.path.isfile(path):
        os.remove(path)


def get_ret_list(ret_obj):
    ret = []
    for kv in ret_obj:
        if isinstance(kv, tuple):
            k, v = kv
            if isinstance(v, NestedDictFS):
                v = v.data_path
            ret.append((k, v))
        elif isinstance(kv, str):
            ret.append(kv)
        else:
            ret.append(kv.data_path)
    return ret


def get_keys(*keys):
    return [k if isinstance(k, tuple) else (k,) for k in keys]


def get_key_path(obj, *keys):
    keys = get_keys(*keys)
    return [(k, obj.key_path(k)) for k in keys]


class TestNestedDictFSViolations(unittest.TestCase):
    def setUp(self):
        self.path = os.path.join(tempfile.gettempdir(), random_folder())
        clean(self.path)

    def tearDown(self):
        clean(self.path)

    def test_file_exist(self):
        with open(self.path, 'w') as f:
            f.write('test')

        with self.assertRaises(ValueError):
            _ = NestedDictFS(self.path, mode='c')

    def test_not_exist(self):
        with self.assertRaises(ValueError):
            _ = NestedDictFS(self.path, mode='r')

        with self.assertRaises(ValueError):
            _ = NestedDictFS(self.path, mode='w')

    def test_read_only(self):
        k = NestedDictFS(self.path, mode='c')
        k['b'] = 2
        k.set_mode('r')

        with self.assertRaises(NDFSAccessViolation):
            k['a'] = 1

        with self.assertRaises(NDFSAccessViolation):
            del k['b']

        with self.assertRaises(NDFSAccessViolation):
            _ = k.get_child('c')

        with self.assertRaises(NDFSAccessViolation):
            _ = k.copy('b', 'c')

        with self.assertRaises(NDFSAccessViolation):
            _ = k.move('b', 'c')

    def test_not_include_value(self):
        k = NestedDictFS(self.path, mode='c')
        k['a'] = 1

        with self.assertRaises(NDFSLookupError) as av:
            k.get('a', include_data=False)

        self.assertEqual(av.exception.error, NDFSLookupError.Type.NOT_INCLUDE_DATA)

        _ = k.get('a')

        with self.assertRaises(NDFSLookupError) as av:
            k.get('a', include_data=False)

        self.assertEqual(av.exception.error, NDFSLookupError.Type.NOT_INCLUDE_DATA)

    def test_not_include_child(self):
        k = NestedDictFS(self.path, mode='c')
        k['a', 'b'] = 1

        with self.assertRaises(NDFSLookupError) as av:
            k.get('a', include_child=False)

        self.assertEqual(av.exception.error, NDFSLookupError.Type.NOT_INCLUDE_CHILD)

        _ = k.get('a')

        with self.assertRaises(NDFSLookupError) as av:
            k.get('a', include_child=False)

        self.assertEqual(av.exception.error, NDFSLookupError.Type.NOT_INCLUDE_CHILD)

    def test_no_suck_key(self):
        k = NestedDictFS(self.path, mode='c')

        with self.assertRaises(NDFSKeyError) as av:
            _ = k['a']
        self.assertEqual(av.exception.error, NDFSKeyError.Type.NO_SUCH_KEY)

        with self.assertRaises(NDFSKeyError) as av:
            del k['a']
        self.assertEqual(av.exception.error, NDFSKeyError.Type.NO_SUCH_KEY)

    def test_bad_path(self):
        with self.assertRaises(TypeError):
            _ = NestedDictFS(None, mode='c')

    def test_ellipsis(self):
        k = NestedDictFS(self.path, mode='c')
        with self.assertRaises(NDFSKeyError) as av:
            _ = k['a', ..., 'b']
        self.assertEqual(av.exception.error, NDFSKeyError.Type.ELLIPSIS)
        with self.assertRaises(NDFSKeyError) as av:
            _ = k['a', ...]
        self.assertEqual(av.exception.error, NDFSKeyError.Type.ELLIPSIS)
        with self.assertRaises(NDFSKeyError) as av:
            _ = k[...]
        self.assertEqual(av.exception.error, NDFSKeyError.Type.ELLIPSIS)

    def test_slice(self):
        k = NestedDictFS(self.path, mode='c')
        with self.assertRaises(NDFSKeyError) as av:
            _ = k['a', :, 'b']
        self.assertEqual(av.exception.error, NDFSKeyError.Type.SLICE)
        with self.assertRaises(NDFSKeyError) as av:
            _ = k['a', :]
        self.assertEqual(av.exception.error, NDFSKeyError.Type.SLICE)
        with self.assertRaises(NDFSKeyError) as av:
            _ = k[:]
        self.assertEqual(av.exception.error, NDFSKeyError.Type.SLICE)

    def test_store_child(self):
        k = NestedDictFS(self.path, mode='c')
        a = k.get_child('a')
        with self.assertRaises(ValueError):
            k['b'] = a

    def test_store_value_over_child(self):
        k = NestedDictFS(self.path, mode='c')
        k['a', 'b'] = 1
        with self.assertRaises(NDFSLookupError) as av:
            k['a'] = 1
        self.assertEqual(av.exception.error, NDFSLookupError.Type.SET_DATA_OVER_CHILD)

    def test_store_child_over_value(self):
        k = NestedDictFS(self.path, mode='c')
        k['a'] = 1
        with self.assertRaises(NDFSLookupError) as av:
            k['a', 'b'] = 1
        self.assertEqual(av.exception.error, NDFSLookupError.Type.DATA_SUB_ITEM)

        with self.assertRaises(NDFSLookupError) as av:
            k.get_child(('a', 'b'))
        self.assertEqual(av.exception.error, NDFSLookupError.Type.DATA_SUB_ITEM)

    def test_invalid_engine(self):
        with self.assertRaises(ValueError):
            _ = NestedDictFS(self.path, mode='c', store_engine='invalid')

        with self.assertRaises(TypeError):
            _ = NestedDictFS(self.path, mode='c', store_engine={})

    def test_move_value_over_child(self):
        k = NestedDictFS(self.path, mode='c')
        k['a', 'x'] = 1
        k['b', 'y'] = 2
        with self.assertRaises(NDFSLookupError) as av:
            k.move(('a', 'x'), 'b')
        self.assertEqual(av.exception.error, NDFSLookupError.Type.SET_DATA_OVER_CHILD)

    def test_copy_value_over_child(self):
        k = NestedDictFS(self.path, mode='c')
        k['a', 'x'] = 1
        k['b', 'y'] = 2
        with self.assertRaises(NDFSLookupError) as av:
            k.copy(('a', 'x'), 'b')
        self.assertEqual(av.exception.error, NDFSLookupError.Type.SET_DATA_OVER_CHILD)

    def test_move_child_over_value(self):
        k = NestedDictFS(self.path, mode='c')
        k['a', 1] = 1
        k['a', 2] = 2
        k['a', 'x', 3] = 3
        k['b'] = 5
        with self.assertRaises(NDFSLookupError) as av:
            k.move('a', 'b')
        self.assertEqual(av.exception.error, NDFSLookupError.Type.SET_CHILD_OVER_DATA)

    def test_copy_child_over_value(self):
        k = NestedDictFS(self.path, mode='c')
        k['a', 1] = 1
        k['a', 2] = 2
        k['a', 'x', 3] = 3
        k['b'] = 5
        with self.assertRaises(NDFSLookupError) as av:
            k.copy('a', 'b')
        self.assertEqual(av.exception.error, NDFSLookupError.Type.SET_CHILD_OVER_DATA)

    def test_move_child_over_child(self):
        k = NestedDictFS(self.path, mode='c')
        k['a', 1] = 1
        k['a', 2] = 2
        k['a', 'x', 3] = 3
        k['b', 'y'] = 5
        with self.assertRaises(NDFSLookupError) as av:
            k.move('a', 'b')
        self.assertEqual(av.exception.error, NDFSLookupError.Type.SET_CHILD_OVER_CHILD)

        k['b'].delete('y')
        self.assertTrue(k['b'].empty())
        k.move('a', 'b')
        self.assertEqual(k['b', 'x', 3], 3)

    def test_copy_child_over_child(self):
        k = NestedDictFS(self.path, mode='c')
        k['a', 1] = 1
        k['a', 2] = 2
        k['a', 'x', 3] = 3
        k['b', 'y'] = 5
        with self.assertRaises(NDFSLookupError) as av:
            k.copy('a', 'b')
        self.assertEqual(av.exception.error, NDFSLookupError.Type.SET_CHILD_OVER_CHILD)

        k['b'].delete('y')
        self.assertTrue(k['b'].empty())
        k.copy('a', 'b')
        self.assertEqual(k['b', 'x', 3], 3)

    def test_copy_non_exist(self):
        k = NestedDictFS(self.path, mode='c')
        with self.assertRaises(NDFSKeyError) as av:
            k.copy('a', 'b')
        self.assertEqual(av.exception.error, NDFSKeyError.Type.NO_SUCH_KEY)

    def test_move_non_exist(self):
        k = NestedDictFS(self.path, mode='c')
        with self.assertRaises(NDFSKeyError) as av:
            k.move('a', 'b')
        self.assertEqual(av.exception.error, NDFSKeyError.Type.NO_SUCH_KEY)

    def test_invalid_key(self):
        k = NestedDictFS(self.path, mode='c')
        invalid_keys = [os.path.join(*k) for k in (('a', 'b'), ('a', '.'), ('a', '..'), ('a', '..', '..'), ('a', '.b'))]
        invalid_keys += ['.', '..']
        valid_keys = '.a..', 'a.b', 'a..b'

        for key in invalid_keys:
            with self.assertRaises(NDFSKeyError) as av:
                k[key] = key
            self.assertEqual(av.exception.error, NDFSKeyError.Type.INVALID_KEY)

        for key in valid_keys:
            k[key] = key

        for key in valid_keys:
            self.assertEqual(k[key], key)


class TestNestedDictFS(unittest.TestCase):
    def setUp(self):
        self.path = os.path.join(tempfile.gettempdir(), random_folder())
        clean(self.path)
        self.k = NestedDictFS(self.path, mode='c')

    def tearDown(self):
        clean(self.path)

    def test_self_init(self):
        k = NestedDictFS(self.path, mode='c', store_engine='msgpack', compress_level=0)
        k['a'] = 1
        new_k = NestedDictFS(k, mode='r')
        self.assertEqual(k.data_path, new_k.data_path)
        self.assertEqual(k.cache, new_k.cache)
        self.assertEqual(k.compress_level, new_k.compress_level)
        self.assertEqual(new_k['a'], 1)

    def test_repr(self):
        k = NestedDictFS(self.path, mode='c')
        str_k = repr(k)
        self.assertIn(NestedDictFS.__name__, str_k)
        self.assertIn(self.path, str_k)

    def test_get_child(self):
        k = NestedDictFS(self.path, mode='c')
        c = k.get_child('a')
        self.assertEqual(k.key_path('a'), c.data_path)

    def test_get_value(self):
        k = NestedDictFS(self.path, mode='c')
        k['a'] = 1
        c = k.get_value('a')
        self.assertEqual(c, 1)

    def test_get_default_value(self):
        k = NestedDictFS(self.path, mode='c')
        c = k.get('a')
        self.assertEqual(c, None)

    def test_deleted_cached(self):
        k1 = NestedDictFS(self.path, mode='c')
        k2 = NestedDictFS(self.path, mode='c')
        k1['a'] = 1
        _ = k2['a']
        del k1['a']
        c = k2.get('a')
        self.assertEqual(c, None)

    def test_get_self(self):
        k = NestedDictFS(self.path, mode='c')
        self.assertEqual(k, k[()])

    def test_delete(self):
        k = NestedDictFS(self.path, mode='c')

        k['a'] = 1
        k['b', 'c'] = 2
        self.assertEqual(k['a'], 1)
        self.assertEqual(k['b', 'c'], 2)

        del k['a']
        del k['b']
        self.assertEqual(k.get('a'), None)
        self.assertEqual(k.get('b'), None)
        self.assertEqual(k.get(('b', 'c')), None)

    def test_keys(self):
        k = NestedDictFS(self.path, mode='c')
        k['a'] = 1
        k['b', 'c'] = 3

        keys = list(k.keys())
        value_keys = list(k.data_keys())
        child_keys = list(k.child_keys())
        self.assertCountEqual(keys, ['a', 'b'])
        self.assertCountEqual(value_keys, ['a'])
        self.assertCountEqual(child_keys, ['b'])

        iter_keys = list(k)
        self.assertCountEqual(iter_keys, [('a',), ('b',)])

    def test_items(self):
        k = NestedDictFS(self.path, mode='c')
        k['a'] = 1
        k['b', 'c'] = 3

        items = get_ret_list(k.items())
        value_items = get_ret_list(k.data_items())
        child_items = get_ret_list(k.child_items())

        expected_value_items = [(('a',), 1)]
        expected_child_items = get_key_path(k, 'b')
        expected_items = [*expected_child_items, *expected_value_items]
        self.assertCountEqual(items, expected_items)
        self.assertCountEqual(expected_value_items, value_items)
        self.assertCountEqual(expected_child_items, child_items)

    def test_append(self):
        k = NestedDictFS(self.path, mode='c')

        k['a'] = 1
        self.assertEqual(k['a'], 1)

        k.append('a', 2)
        self.assertListEqual(k['a'], [1, 2])

        k.append('a', 3)
        self.assertListEqual(k['a'], [1, 2, 3])

        k = NestedDictFS(self.path, mode='c', store_engine='msgpack')

        k['a'] = 1
        self.assertEqual(k['a'], 1)

        k.append('a', 2)
        self.assertListEqual(k['a'], [1, 2])

        k.append('a', 3)
        self.assertListEqual(k['a'], [1, 2, 3])

    def test_exists(self):
        k = NestedDictFS(self.path, mode='c')
        k['a'] = 1
        k['b', 'c'] = 2
        self.assertTrue(k.exists('a'))
        self.assertTrue('a' in k)
        self.assertTrue(k.value_exists('a'))
        self.assertFalse(k.child_exists('a'))

        self.assertTrue(k.exists('b'))
        self.assertTrue('b' in k)
        self.assertFalse(k.value_exists('b'))
        self.assertTrue(k.child_exists('b'))

    def test_update_cache(self):
        k1 = NestedDictFS(self.path, mode='c')
        k2 = NestedDictFS(self.path, mode='c')
        p = k2.key_path('a')

        k1['a'] = {'a': 1}
        c1 = k2['a']
        c2 = k2['a']
        self.assertTrue(c1 is c2)

        k1['a'] = 2
        self.assertEqual(k2['a'], 2)

    def test_clear_cache(self):
        k = NestedDictFS(self.path, mode='c')
        k['a'] = 1
        _ = k['a']
        self.assertGreaterEqual(len(k.cache), 1)
        k.clear_cache()
        self.assertEqual(len(k.cache), 0)

    def test_msgpack(self):
        k = NestedDictFS(self.path, mode='c', store_engine='msgpack')
        k['a'] = 1
        self.assertEqual(k['a'], 1)

        with self.assertRaises(TypeError):
            k['a'] = np.array([1, 2, 3])

    def test_msgpack_numpy(self):
        k = NestedDictFS(self.path, mode='c', store_engine='msgpack-numpy')
        a = np.array([1, 2, 3])
        k['a'] = a
        self.assertTrue(np.all(k['a'] == a))

    def test_pickle(self):
        k = NestedDictFS(self.path, mode='c', store_engine='pickle')
        k['a'] = 1
        self.assertEqual(k['a'], 1)

    def test_binary(self):
        k = NestedDictFS(self.path, mode='c', store_engine='binary')
        k['a'] = b"test"
        self.assertEqual(k['a'], b"test")

    def test_manual_storage_engine(self):
        import pickle

        def my_write(f, obj):
            pickle.dump(obj, f)

        def my_read(f):
            return pickle.load(f)

        k = NestedDictFS(self.path, mode='c', store_engine=(my_write, my_read))
        k['a'] = 1
        self.assertEqual(k['a'], 1)

    def test_move_value(self):
        k = NestedDictFS(self.path, mode='c')
        k['a', 'b'] = 1
        k.move(('a', 'b'), 'd')
        self.assertEqual(k['d'], 1)
        self.assertTrue(k.exists('a'))
        self.assertFalse(k.exists(('a', 'b')))

    def test_copy_value(self):
        k = NestedDictFS(self.path, mode='c')
        k['a', 'b'] = 1
        k.copy(('a', 'b'), 'd')
        self.assertEqual(k['d'], 1)
        self.assertTrue(k.exists(('a', 'b')))

    def test_move_child(self):
        k = NestedDictFS(self.path, mode='c')
        k['a', 1] = 1
        k['a', 2] = 2
        k['a', 'c', 3] = 3
        k.move('a', 'b')
        self.assertEqual(k['b', 1], 1)
        self.assertEqual(k['b', 2], 2)
        self.assertEqual(k['b', 'c', 3], 3)
        self.assertFalse(k.exists('a'))

    def test_copy_child(self):
        k = NestedDictFS(self.path, mode='c')
        k['a', 1] = 1
        k['a', 2] = 2
        k['a', 'c', 3] = 3
        k.copy('a', 'b')
        self.assertEqual(k['b', 1], 1)
        self.assertEqual(k['b', 2], 2)
        self.assertEqual(k['b', 'c', 3], 3)
        self.assertTrue(k.exists('a'))
        self.assertEqual(k['a', 1], 1)
        self.assertEqual(k['a', 2], 2)
        self.assertEqual(k['a', 'c', 3], 3)

    def test_len(self):
        k = NestedDictFS(self.path, mode='c')
        expected_len = 5
        for i in range(expected_len):
            k[i] = i
        self.assertEqual(k.len(), expected_len)
        self.assertEqual(len(k), expected_len)

        self.assertEqual(len(k.get_child('a')), 0)


def get_key_value(*keys):
    keys = [k if isinstance(k, tuple) else (k,) for k in keys]
    return [(k, f"Value={k}") for k in keys]


class FakeQuery:
    def __getitem__(self, item):
        return item


fq = FakeQuery()


class TestNestedDictFSGetSlice(unittest.TestCase):
    def setUp(self):
        self.path = os.path.join(tempfile.gettempdir(), random_folder())
        clean(self.path)

        d = {}
        d['v'] = "Dummy"
        for i1 in ('a', 'b', 'c'):
            d.setdefault(i1, {})['v'] = f"Dummy={i1}"
            for i2 in ('1', '2', '3'):
                for i3 in ('X', 'Y', 'Z'):
                    d.setdefault(i1, {}).setdefault(i2, {})[i3] = f"Value={(i1, i2, i3)}"
        self.d = d

        k = NestedDictFS(self.path, mode='c')
        k.update(d, 10)

        # k['v'] = "Dummy"
        # for i1 in ('a', 'b', 'c'):
        #     k[i1, 'v'] = f"Dummy={i1}"
        #     for i2 in ('1', '2', '3'):
        #         for i3 in ('X', 'Y', 'Z'):
        #             k[i1, i2, i3] = f"Value={(i1, i2, i3)}"

        self.k = NestedDictFS(self.path, mode='c')

    def tearDown(self):
        clean(self.path)

    def _get_key_path(self, *keys):
        return get_key_path(self.k, *keys)

    def _get_key_value(self, key):
        d = self.d
        for k in key:
            d = d[k]
        return d

    def _generic_query(self, query, *expected_keys):
        expected_keys = get_keys(*expected_keys)
        expected_values = [self._get_key_value(k) for k in expected_keys]

        expected_child_items = [(k, self.k.key_path(k)) for k, v in zip(expected_keys, expected_values) if
                                isinstance(k, dict)]
        expected_data_items = [(k, v) for k, v in zip(expected_keys, expected_values) if not isinstance(k, dict)]

        expected_child_keys = [k for k, v in expected_child_items]
        expected_data_keys = [k for k, v in expected_data_items]

        expected_child_values = [v for k, v in expected_child_items]
        expected_data_values = [v for k, v in expected_data_items]

        ret = get_ret_list(self.k.items[query])
        self.assertCountEqual(ret, expected_child_items + expected_data_items)

        ret = get_ret_list(self.k.child_items[query])
        self.assertCountEqual(ret, expected_child_items)

        ret = get_ret_list(self.k.data_items[query])
        self.assertCountEqual(ret, expected_data_items)

        ret = get_ret_list(self.k.keys[query])
        self.assertCountEqual(ret, expected_keys)

        ret = get_ret_list(self.k.child_keys[query])
        self.assertCountEqual(ret, expected_child_keys)

        ret = get_ret_list(self.k.data_keys[query])
        self.assertCountEqual(ret, expected_data_keys)

        ret = get_ret_list(self.k.values[query])
        self.assertCountEqual(ret, expected_values)

        ret = get_ret_list(self.k.child_values[query])
        self.assertCountEqual(ret, expected_child_values)

        ret = get_ret_list(self.k.data_values[query])
        self.assertCountEqual(ret, expected_data_values)

    def test_all(self):
        self._generic_query(fq[:],
                            'a', 'b', 'c', 'v')

    def test_sub_double_nested(self):
        self._generic_query(fq['a', :, :],
                            *itertools.product(('a',), ('1', '2', '3'), ('X', 'Y', 'Z')))

    def test_first_double_nested(self):
        self._generic_query(fq[:, :, 'X'],
                            *itertools.product(('a', 'b', 'c'), ('1', '2', '3'), ('X',)))

    def test_dummy_child(self):
        self._generic_query(fq[:, 'v'],
                            *itertools.product(('a', 'b', 'c'), ('v',)))

    def test_middle(self):
        self._generic_query(fq['a', :, 'X'],
                            *itertools.product(('a',), ('1', '2', '3'), ('X',)))

    def test_sub_single(self):
        self._generic_query(fq['a', :],
                            *itertools.product(('a',), ('1', '2', '3')))

    def test_first_single(self):
        self._generic_query(fq[:, '1'],
                            *itertools.product(('a', 'b', 'c'), ('1',)))

    def test_sandwich(self):
        self._generic_query(fq[:, '1', :],
                            *itertools.product(('a', 'b', 'c'), ('1',), ('X', 'Y', 'Z')))

    def test_asymmetric(self):
        val = 'asymmetric'
        self.k['a', 'e'] = val
        ret = get_ret_list(self.k.items[:, 'e'])
        expected_ret = [(('a', 'e'), val)]
        self.assertCountEqual(ret, expected_ret)
