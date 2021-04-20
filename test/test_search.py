"""
Author: Liran Funaro <liran.funaro@gmail.com>

Copyright (C) 2006-2021 Liran Funaro

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
import itertools
import re


class FakeQuery:
    def __getitem__(self, item):
        return item


fq = FakeQuery()


class TestNestedDictFSSearch(unittest.TestCase):
    def setUp(self):
        self.path = setup_test()

        d = {'v': 'Dummy'}
        for i1 in ('a', 'b', 'c'):
            d.setdefault(i1, {})['v'] = f"Dummy={i1}"
            for i2 in ('1', '2', '3'):
                for i3 in ('X', 'Y', 'Z'):
                    d.setdefault(i1, {}).setdefault(i2, {})[i3] = f"Value={(i1, i2, i3)}"
        self.d = d

        k = DictFS(self.path, mode='c')
        k.update(d, 10)

        self.k = DictFS(self.path, mode='c')
        self.maxDiff = None

    def tearDown(self):
        clean(self.path)

    def _get_key_value(self, key):
        d = self.d
        for k in key:
            d = d[k]
        return d

    def _generic_query(self, query, *expected_keys):
        expected_keys = get_keys(*expected_keys)
        expected_values = [self._get_key_value(k) for k in expected_keys]
        is_child = [isinstance(v, dict) for v in expected_values]
        expected_values = [v if not c else self.k.key_path(k) for k, v, c in
                           zip(expected_keys, expected_values, is_child)]

        expected_child_items = [(k, v) for k, v, c in zip(expected_keys, expected_values, is_child) if c]
        expected_data_items = [(k, v) for k, v, c in zip(expected_keys, expected_values, is_child) if not c]

        expected_child_keys = [k for k, v in expected_child_items]
        expected_data_keys = [k for k, v in expected_data_items]

        expected_child_values = [v for k, v in expected_child_items]
        expected_data_values = [v for k, v in expected_data_items]

        ret = get_ret_list_items(self.k.items[query])
        self.assertCountEqual(ret, expected_child_items + expected_data_items)

        ret = get_ret_list_items(self.k.child_items[query])
        self.assertCountEqual(ret, expected_child_items)

        ret = get_ret_list_items(self.k.data_items[query])
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
                            *itertools.product(('a',), ('1', '2', '3', 'v')))

    def test_first_single(self):
        self._generic_query(fq[:, '1'],
                            *itertools.product(('a', 'b', 'c'), ('1',)))

    def test_sandwich(self):
        self._generic_query(fq[:, '1', :],
                            *itertools.product(('a', 'b', 'c'), ('1',), ('X', 'Y', 'Z')))

    def test_regexp(self):
        self._generic_query(fq[re.compile(r'[ab]'), '1', :],
                            *itertools.product(('a', 'b'), ('1',), ('X', 'Y', 'Z')))

    def test_asymmetric(self):
        val = 'asymmetric'
        self.k['a', 'e'] = val
        ret = get_ret_list_items(self.k.items[:, 'e'])
        expected_ret = [(('a', 'e'), val)]
        self.assertCountEqual(ret, expected_ret)

    def test_walk(self):
        ret = list(self.k.walk(yield_values=False))
        expected_keys = get_keys(())
        expected_keys.extend(get_keys('a', 'b', 'c', 'v'))
        expected_keys.extend(get_keys(*itertools.product(('a', 'b', 'c'), ('1', '2', '3', 'v'))))
        expected_keys.extend(get_keys(*itertools.product(('a', 'b', 'c'), ('1', '2', '3'), ('X', 'Y', 'Z'))))
        self.assertCountEqual(ret, expected_keys)

        ret = list(self.k.walk(yield_values=False, include_child=False))
        expected_keys = get_keys('v')
        expected_keys.extend(get_keys(*itertools.product(('a', 'b', 'c'), ('v',))))
        expected_keys.extend(get_keys(*itertools.product(('a', 'b', 'c'), ('1', '2', '3'), ('X', 'Y', 'Z'))))
        self.assertCountEqual(ret, expected_keys)

        ret = list(self.k.walk(yield_values=False, include_data=False))
        expected_keys = get_keys(())
        expected_keys.extend(get_keys('a', 'b', 'c'))
        expected_keys.extend(get_keys(*itertools.product(('a', 'b', 'c'), ('1', '2', '3'))))
        self.assertCountEqual(ret, expected_keys)

    def test_slice_and_walk(self):
        ret = get_ret_list(self.k.keys[..., 'v'])
        expected_keys = get_keys('v')
        expected_keys.extend(get_keys(*itertools.product(('a', 'b', 'c'), ('v',))))
        self.assertCountEqual(ret, expected_keys)

    def test_empty_search(self):
        k = DictFS(self.path, mode='c')
        ret = get_ret_list(k.search((), yield_keys=True, yield_values=False))
        expected_keys = get_keys(())
        self.assertCountEqual(ret, expected_keys)
