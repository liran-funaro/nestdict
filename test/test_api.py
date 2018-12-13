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
from test import *
import unittest


class TestNestedDictFS(unittest.TestCase):
    def setUp(self):
        self.path = setup_test()
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

        expected_data_keys = get_keys('a')
        expected_child_keys = get_keys('b')

        keys = list(k.keys())
        data_keys = list(k.data_keys())
        child_keys = list(k.child_keys())
        self.assertCountEqual(keys, expected_data_keys + expected_child_keys)
        self.assertCountEqual(data_keys, expected_data_keys)
        self.assertCountEqual(child_keys, expected_child_keys)

        keys = list(k.keys)
        data_keys = list(k.data_keys)
        child_keys = list(k.child_keys)
        self.assertCountEqual(keys, expected_data_keys + expected_child_keys)
        self.assertCountEqual(data_keys, expected_data_keys)
        self.assertCountEqual(child_keys, expected_child_keys)

        iter_keys = list(k)
        self.assertCountEqual(iter_keys, ['a', 'b'])

    def test_values(self):
        k = NestedDictFS(self.path, mode='c')
        k['a'] = 1
        k['b', 'c'] = 3

        expected_data_values = [1]
        expected_child_values = [self.k.key_path('b')]
        expected_values = [*expected_child_values, *expected_data_values]

        data_values = get_ret_list(k.data_values())
        child_values = get_ret_list(k.child_values())
        values = get_ret_list(k.values())
        self.assertCountEqual(data_values, expected_data_values)
        self.assertCountEqual(child_values, expected_child_values)
        self.assertCountEqual(values, expected_values)

        data_values = get_ret_list(k.data_values)
        child_values = get_ret_list(k.child_values)
        values = get_ret_list(k.values)
        self.assertCountEqual(data_values, expected_data_values)
        self.assertCountEqual(child_values, expected_child_values)
        self.assertCountEqual(values, expected_values)

    def test_items(self):
        k = NestedDictFS(self.path, mode='c')
        k['a'] = 1
        k['b', 'c'] = 3

        expected_data_items = [('a', 1)]
        expected_child_items = get_key_path(k, 'b')
        expected_items = [*expected_child_items, *expected_data_items]

        items = get_ret_list_items(k.items())
        data_items = get_ret_list_items(k.data_items())
        child_items = get_ret_list_items(k.child_items())
        self.assertCountEqual(items, expected_items)
        self.assertCountEqual(expected_data_items, data_items)
        self.assertCountEqual(expected_child_items, child_items)

        items = get_ret_list_items(k.items)
        data_items = get_ret_list_items(k.data_items)
        child_items = get_ret_list_items(k.child_items)
        self.assertCountEqual(items, expected_items)
        self.assertCountEqual(expected_data_items, data_items)
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

    def test_get_cached(self):
        self.k['a'] = 1
        self.assertEqual(self.k.get_cached('a'), 1)
        self.assertEqual(self.k.get_cached(()).data_path, self.path)

    def test_get_direct(self):
        self.k['a'] = 1
        self.assertEqual(self.k.get_direct('a'), 1)
        self.assertEqual(self.k.get_direct(()).data_path, self.path)

    def test_update(self):
        self.k.update({'a': 1, 'b': {'c': 2}}, max_depth=0)
        self.assertEqual(self.k['a'], 1)
        self.assertEqual(self.k['b'], {'c': 2})

        for k in self.k.keys():
            del self.k[k]
        self.k.update({'a': 1, 'b': {'c': 2}}, max_depth=1)
        self.assertEqual(self.k['a'], 1)
        self.assertEqual(self.k['b'].data_path, self.k.key_path('b'))

    def test_path_key(self):
        p = self.k.path_key(self.path)
        self.assertEqual(p, ())

        p = self.k.path_key(os.path.join(self.path, '.'))
        self.assertEqual(p, ())

        p = self.k.path_key(os.path.join(self.path, 'a'))
        self.assertEqual(p, 'a')

        p = self.k.path_key(os.path.join(self.path, 'a', 'b'))
        self.assertEqual(p, ('a', 'b'))

