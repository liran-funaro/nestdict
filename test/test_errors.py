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


class TestNestedDictFSErrors(unittest.TestCase):
    def setUp(self):
        self.path = setup_test()

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

    def test_invalid_update(self):
        k = NestedDictFS(self.path, mode='c')
        with self.assertRaises(ValueError):
            k.update('a')

    def test_empty_search(self):
        k = NestedDictFS(self.path, mode='c')
        with self.assertRaises(ValueError):
            list(k.search((), yield_keys=True, yield_values=True))

    def test_not_sub_path_key(self):
        k = NestedDictFS(self.path, mode='c')
        with self.assertRaises(ValueError):
            k.path_key(os.path.join(self.path, '..'))
