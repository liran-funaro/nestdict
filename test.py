import os
import uuid
import unittest
import shutil
import tempfile
import itertools
import numpy as np

from nested_dict_fs import NestedDictFS, AccessViolation, AccessViolationType


def random_folder():
    return uuid.uuid4().hex.upper()[:8]


def clean(path):
    if os.path.isdir(path):
        shutil.rmtree(path)
    elif os.path.isfile(path):
        os.remove(path)


def get_ret_list(ret_obj):
    ret = []
    for k, v in ret_obj:
        if isinstance(v, NestedDictFS):
            v = v.data_path
        ret.append((k, v))
    return ret


def get_key_path(obj, *keys, force_tuple=True):
    if force_tuple:
        keys = [k if isinstance(k, tuple) else (k,) for k in keys]
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

        with self.assertRaises(AccessViolation) as av:
            k['a'] = 1
        self.assertEqual(av.exception.violation, AccessViolationType.READ_ONLY)

        with self.assertRaises(AccessViolation) as av:
            del k['b']
        self.assertEqual(av.exception.violation, AccessViolationType.READ_ONLY)

    def test_not_include_value(self):
        k = NestedDictFS(self.path, mode='c')
        k['a'] = 1

        with self.assertRaises(AccessViolation) as av:
            k.get('a', include_value=False)

        self.assertEqual(av.exception.violation, AccessViolationType.NOT_INCLUDE_VALUE)

        _ = k.get('a')

        with self.assertRaises(AccessViolation) as av:
            k.get('a', include_value=False)

        self.assertEqual(av.exception.violation, AccessViolationType.NOT_INCLUDE_VALUE)

    def test_not_include_child(self):
        k = NestedDictFS(self.path, mode='c')
        k['a', 'b'] = 1

        with self.assertRaises(AccessViolation) as av:
            k.get('a', include_child=False)

        self.assertEqual(av.exception.violation, AccessViolationType.NOT_INCLUDE_CHILD)

        _ = k.get('a')

        with self.assertRaises(AccessViolation) as av:
            k.get('a', include_child=False)

        self.assertEqual(av.exception.violation, AccessViolationType.NOT_INCLUDE_CHILD)

    def test_no_suck_key(self):
        k = NestedDictFS(self.path, mode='c')

        with self.assertRaises(AccessViolation) as av:
            _ = k['a']
        self.assertEqual(av.exception.violation, AccessViolationType.NO_SUCH_KEY)

        with self.assertRaises(AccessViolation) as av:
            del k['a']
        self.assertEqual(av.exception.violation, AccessViolationType.NO_SUCH_KEY)

    def test_bad_path(self):
        with self.assertRaises(TypeError):
            _ = NestedDictFS(None, mode='c')

    def test_get_child(self):
        k = NestedDictFS(self.path, mode='c')
        k.set_mode('r')
        with self.assertRaises(AccessViolation) as av:
            _ = k.get_child('a')
        self.assertEqual(av.exception.violation, AccessViolationType.READ_ONLY)

    def test_middle_ellipsis(self):
        k = NestedDictFS(self.path, mode='c')
        with self.assertRaises(KeyError):
            _ = k['a', ..., 'b']

    def test_store_child(self):
        k = NestedDictFS(self.path, mode='c')
        a = k.get_child('a')
        with self.assertRaises(ValueError):
            k['b'] = a

    def test_store_value_over_child(self):
        k = NestedDictFS(self.path, mode='c')
        k['a', 'b'] = 1
        with self.assertRaises(AccessViolation) as av:
            k['a'] = 1
        self.assertEqual(av.exception.violation, AccessViolationType.SET_CHILD)

    def test_store_child_over_value(self):
        k = NestedDictFS(self.path, mode='c')
        k['a'] = 1
        with self.assertRaises(AccessViolation) as av:
            k['a', 'b'] = 1
        self.assertEqual(av.exception.violation, AccessViolationType.VALUE_SUB_ITEM)

        with self.assertRaises(AccessViolation) as av:
            k.get_child(('a', 'b'))
        self.assertEqual(av.exception.violation, AccessViolationType.VALUE_SUB_ITEM)

    def test_invalid_engine(self):
        with self.assertRaises(ValueError):
            _ = NestedDictFS(self.path, mode='c', store_engine='invalid')

        with self.assertRaises(TypeError):
            _ = NestedDictFS(self.path, mode='c', store_engine={})


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
        self.assertEqual(k, k[...])

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
        value_keys = list(k.value_keys())
        child_keys = list(k.child_keys())
        self.assertCountEqual(keys, ['a', 'b'])
        self.assertCountEqual(value_keys, ['a'])
        self.assertCountEqual(child_keys, ['b'])

        iter_keys = list(k)
        self.assertCountEqual(iter_keys, ['a', 'b'])

    def test_items(self):
        k = NestedDictFS(self.path, mode='c')
        k['a'] = 1
        k['b', 'c'] = 3

        items = get_ret_list(k.items())
        value_items = get_ret_list(k.value_items())
        child_items = get_ret_list(k.child_items())

        expected_value_items = [('a', 1)]
        expected_child_items = get_key_path(k, 'b', force_tuple=False)
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


def get_key_value(*keys):
    keys = [k if isinstance(k, tuple) else (k,) for k in keys]
    return [(k, f"Value={k}") for k in keys]


class TestNestedDictFSGetSlice(unittest.TestCase):
    def setUp(self):
        self.path = os.path.join(tempfile.gettempdir(), random_folder())
        clean(self.path)
        k = NestedDictFS(self.path, mode='c')
        k['v'] = "Dummy"
        for i1 in ('a', 'b', 'c'):
            k[i1, 'v'] = f"Dummy={i1}"
            for i2 in ('1', '2', '3'):
                for i3 in ('X', 'Y', 'Z'):
                    k[i1, i2, i3] = f"Value={(i1, i2, i3)}"
        self.k = NestedDictFS(self.path, mode='c')

    def tearDown(self):
        clean(self.path)

    def _get_key_path(self, *keys):
        return get_key_path(self.k, *keys)

    def test_all(self):
        ret = get_ret_list(self.k[:])
        expected_ret = self._get_key_path('a', 'b', 'c')
        expected_ret.append((('v',), 'Dummy'))
        self.assertCountEqual(ret, expected_ret)

    def test_all_ellipsis(self):
        ret = get_ret_list(self.k[:, ...])
        expected_ret = self._get_key_path('a', 'b', 'c')
        self.assertCountEqual(ret, expected_ret)

    def test_sub_double_nested(self):
        ret = get_ret_list(self.k['a', :, :])
        expected_ret = get_key_value(*itertools.product(('a',), ('1', '2', '3'), ('X', 'Y', 'Z')))
        self.assertCountEqual(ret, expected_ret)

    def test_sub_double_nested_ellipsis(self):
        ret = get_ret_list(self.k['a', :, :, ...])
        self.assertCountEqual(ret, [])

    def test_first_double_nested(self):
        ret = get_ret_list(self.k[:, :, 'X'])
        expected_ret = get_key_value(*itertools.product(('a', 'b', 'c'), ('1', '2', '3'), ('X',)))
        self.assertCountEqual(ret, expected_ret)

    def test_first_double_nested_ellipsis(self):
        with self.assertRaises(AccessViolation) as av:
            _ = get_ret_list(self.k[:, :, 'X', ...])

        self.assertEqual(av.exception.violation, AccessViolationType.NOT_INCLUDE_VALUE)

    def test_dummy_ellipsis(self):
        with self.assertRaises(AccessViolation) as av:
            _ = get_ret_list(self.k[:, 'v', ...])

        self.assertEqual(av.exception.violation, AccessViolationType.NOT_INCLUDE_VALUE)

    def test_middle(self):
        ret = get_ret_list(self.k['a', :, 'X'])
        expected_ret = get_key_value(*itertools.product(('a',), ('1', '2', '3'), ('X',)))
        self.assertCountEqual(ret, expected_ret)

    def test_middle_ellipsis(self):
        with self.assertRaises(AccessViolation) as av:
            _ = get_ret_list(self.k['a', :, 'X', ...])

        self.assertEqual(av.exception.violation, AccessViolationType.NOT_INCLUDE_VALUE)

    def test_sub_single(self):
        ret = get_ret_list(self.k['a', :])
        expected_ret = self._get_key_path(*itertools.product(('a',), ('1', '2', '3')))
        expected_ret.append((('a', 'v'), 'Dummy=a'))
        self.assertCountEqual(ret, expected_ret)

    def test_sub_single_ellipsis(self):
        ret = get_ret_list(self.k['a', :, ...])
        expected_ret = self._get_key_path(*itertools.product(('a',), ('1', '2', '3')))
        self.assertCountEqual(ret, expected_ret)

    def test_first_single(self):
        ret = get_ret_list(self.k[:, '1'])
        expected_ret = self._get_key_path(*itertools.product(('a', 'b', 'c'), ('1',)))
        self.assertCountEqual(ret, expected_ret)

    def test_first_single_ellipsis(self):
        ret = get_ret_list(self.k[:, '1', ...])
        expected_ret = self._get_key_path(*itertools.product(('a', 'b', 'c'), ('1',)))
        self.assertCountEqual(ret, expected_ret)

    def test_sandwich(self):
        ret = get_ret_list(self.k[:, '1', :])
        expected_ret = get_key_value(*itertools.product(('a', 'b', 'c'), ('1',), ('X', 'Y', 'Z')))
        self.assertCountEqual(ret, expected_ret)

    def test_sandwich_ellipsis(self):
        ret = get_ret_list(self.k[:, '1', :, ...])
        self.assertCountEqual(ret, [])

    def test_asymmetric(self):
        val = 'asymmetric'
        self.k['a', 'e'] = val
        ret = get_ret_list(self.k[:, 'e'])
        expected_ret = [(('a', 'e'), val)]
        self.assertCountEqual(ret, expected_ret)
