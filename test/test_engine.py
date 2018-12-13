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
import numpy as np


class TestNestedDictFSEngine(unittest.TestCase):
    def setUp(self):
        self.path = setup_test()

    def tearDown(self):
        clean(self.path)

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

    def test_plain(self):
        k = NestedDictFS(self.path, mode='c', store_engine='plain')
        k['a'] = "test"
        self.assertEqual(k['a'], "test")

    def test_manual_storage_engine(self):
        import pickle

        def my_write(f, obj):
            pickle.dump(obj, f)

        def my_read(f):
            return pickle.load(f)

        k = NestedDictFS(self.path, mode='c', store_engine=(my_write, my_read))
        k['a'] = 1
        self.assertEqual(k['a'], 1)
