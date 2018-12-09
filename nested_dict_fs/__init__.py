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
import gc
import gzip
import shutil
from lru import LRU
from enum import Enum, auto
from nested_dict_fs import store_engines


class NDFSException(Exception):
    def __init__(self, obj, item, msg):
        self.obj = obj
        self.item = item
        super().__init__(msg)


class NDFSAccessViolation(NDFSException):
    def __init__(self, obj, item):
        msg = f"{obj} was opened in read-only mode when trying to modify {item}."
        super().__init__(obj, item, msg)


class NDFSLookupError(NDFSException):
    class LookupErrorType(Enum):
        NOT_INCLUDE_CHILD = auto()
        NOT_INCLUDE_VALUE = auto()
        VALUE_SUB_ITEM = auto()
        SET_VALUE_OVER_CHILD = auto()
        SET_CHILD_OVER_VALUE = auto()
        SET_CHILD_OVER_CHILD = auto()

    def __init__(self, obj, item, error: LookupErrorType, sub_item=None):
        msg = f"Lookup error in {obj} with item {item}."
        if error == self.LookupErrorType.NOT_INCLUDE_CHILD:
            msg = f"Item {item} is a child but children were not included in the search."
        elif error == self.LookupErrorType.NOT_INCLUDE_VALUE:
            msg = f"Item {item} is a value but values were not included in the search."
        elif error == self.LookupErrorType.VALUE_SUB_ITEM:
            msg = f"Sub item {sub_item} is a value but requested {item}."
        elif error == self.LookupErrorType.SET_VALUE_OVER_CHILD:
            msg = f"Cannot set/copy/move a value to an existing child. Must remove subtree {item}."
        elif error == self.LookupErrorType.SET_CHILD_OVER_VALUE:
            msg = f"Cannot copy/move a child to an existing value. Must remove value {item}."
        elif error == self.LookupErrorType.SET_CHILD_OVER_CHILD:
            msg = f"Cannot copy/move a child to an existing non empty child. Must remove subtree {item}."

        super().__init__(obj, item, msg)
        self.error = error
        self.sub_item = sub_item


class NDFSKeyError(NDFSException):
    class KeyErrorType(Enum):
        NO_SUCH_KEY = auto()
        INVALID_KEY = auto()
        ELLIPSIS = auto()

    def __init__(self, obj, item, error: KeyErrorType):
        msg = f"Key error in {obj} with item {item}."
        if error == self.KeyErrorType.NO_SUCH_KEY:
            msg = f"Item {item} does not exist in {obj}."
        elif error == self.KeyErrorType.INVALID_KEY:
            msg = f"Invalid key: {item}."
        elif error == self.KeyErrorType.ELLIPSIS:
            msg = "Ellipsis (...) may only be used as the final nested key."

        super().__init__(obj, item, msg)
        self.error = error


class NestedDictFS:
    __slots__ = ('data_path', 'mode', 'writable', 'store_engine', 'write_method', 'read_method', 'compress_level',
                 'cache')

    def __init__(self, data_path, mode='r', cache_size=None, shared_cache=None,
                 store_engine=store_engines.DEFAULT_STORE_ENGINE, compress_level=9):
        """
        :param data_path: The key value store data path. It is possible to start from sub folder of existing path.
            Might be a string represents the path or an existing KeyValueFolderStore object.
        :param mode: Should be 'r' for read only, 'w', 'rw' or 'wr' for writable,
            and 'c' for allow creation of a new data path (lazy).
        :param cache_size: Define the cache size to use. Defaults to 128.
        :param shared_cache: Can be used to pass a cache object from another instance to be shared (ignores cache_size).
        :param store_engine: Use one of the following: msgpack, msgpack-numpy (default)
            The class does not infer the store engine from the data path, so it is
            up to the programmer to know the store engine used for an existing data path.
        :param compress_level: See `gzip`.
        """
        if isinstance(data_path, str):
            self.data_path = data_path
        elif isinstance(data_path, self.__class__):
            parent = data_path
            shared_cache = parent.cache
            store_engine = parent.store_engine
            compress_level = parent.compress_level
            self.data_path = parent.data_path
        else:
            raise TypeError(
                f"data_path must be a string or a {self.__class__.__name__} object. Not a {type(data_path)}.")

        self.mode = mode
        self.writable = any(k in mode for k in 'wc')
        self.store_engine = store_engine
        self.write_method, self.read_method = store_engines.get_store_engine(store_engine)
        self.compress_level = compress_level
        if shared_cache is not None:
            self.cache = shared_cache
        else:
            self.cache = LRU(cache_size or 128)

        create = 'c' in mode
        if os.path.isfile(self.data_path):
            raise ValueError(f"Data path {self.data_path} must be a folder, but it is a file.")
        if not os.path.isdir(self.data_path) and not create:
            raise ValueError(f"Data path {self.data_path} does not exist.")

    def set_mode(self, mode='r'):
        self.mode = mode
        self.writable = any(k in mode for k in 'wc')

    ######################################################################################################
    # Representation interface
    ######################################################################################################

    def __str__(self):
        return f"{self.__class__.__name__}({self.data_path}, {self.mode})"

    def __repr__(self):
        return str(self)

    def key_path(self, key):
        if type(key) in (list, tuple):
            return os.path.join(self.data_path, *map(str, key))
        else:
            return os.path.join(self.data_path, str(key))

    ######################################################################################################
    # Internal helpers
    ######################################################################################################

    def _internal_open(self, filepath, mode='r'):
        if self.compress_level == 0:
            return open(filepath, mode+'b')
        else:
            return gzip.open(filepath, mode+'b', compresslevel=self.compress_level)

    def _internal_write(self, filepath, obj, append=False):
        with self._internal_open(filepath, 'a' if append else 'w') as f:
            self.write_method(f, obj)

    def _internal_read(self, filepath):
        with self._internal_open(filepath, 'r') as f:
            return self.read_method(f)

    def _internal_get_child(self, item_path, create=False):
        mode = self.mode if not create else 'c'
        return self.__class__(item_path, mode=mode, shared_cache=self.cache, store_engine=self.store_engine)

    def _internal_verify_item(self, item):
        if type(item) not in (list, tuple):
            item = (item,)
        item = tuple(map(str, item))
        for k in item:
            if k in ('.', '..') or os.path.sep in k:
                raise NDFSKeyError(self, item, NDFSKeyError.KeyErrorType.INVALID_KEY)

        for i in range(1, len(item)):
            sub_item = item[:i]
            sub_item_path = self.key_path(sub_item)
            if os.path.isdir(sub_item_path):
                continue
            elif os.path.isfile(sub_item_path):
                raise NDFSLookupError(self, item, NDFSLookupError.LookupErrorType.VALUE_SUB_ITEM, sub_item)

    def _internal_get_direct(self, item, item_path, default_value=None, raise_err=True,
                             include_child=True, include_value=True, create_child=False):
        include_child |= create_child
        if os.path.isdir(item_path):
            if not include_child:
                raise NDFSLookupError(self, item, NDFSLookupError.LookupErrorType.NOT_INCLUDE_CHILD)
            return self._internal_get_child(item_path, create=False)

        if os.path.isfile(item_path):
            if not include_value:
                raise NDFSLookupError(self, item, NDFSLookupError.LookupErrorType.NOT_INCLUDE_VALUE)
            return self._internal_read(item_path)

        if create_child:
            if self.writable:
                return self._internal_get_child(item_path, create=True)
            else:
                raise NDFSAccessViolation(self, item)
        elif raise_err:
            raise NDFSKeyError(self, item, NDFSKeyError.KeyErrorType.NO_SUCH_KEY)

        return default_value

    def _internal_get_cached(self, item, default_value=None, raise_err=True,
                             include_child=True, include_value=True, create_child=False):
        self._internal_verify_item(item)
        item_path = self.key_path(item)

        ret_stat, ret_value = self.cache.get(item_path, (None, default_value))
        try:
            cur_stat = os.stat(item_path)
        except FileNotFoundError:
            cur_stat = None
        if ret_stat is not None and ret_stat != cur_stat:
            del self.cache[item_path]
            ret_value = default_value
            ret_stat = None
        if ret_stat is None:
            ret_value = self._internal_get_direct(item, item_path, default_value, raise_err,
                                                  include_child, include_value, create_child)
            if ret_value is not default_value and cur_stat is not None:
                self.cache[item_path] = (cur_stat, ret_value)

        is_child = isinstance(ret_value, self.__class__)
        if is_child and not include_child:
            raise NDFSLookupError(self, item, NDFSLookupError.LookupErrorType.NOT_INCLUDE_CHILD)
        elif not is_child and not include_value and ret_value is not default_value:
            raise NDFSLookupError(self, item, NDFSLookupError.LookupErrorType.NOT_INCLUDE_VALUE)

        return ret_value

    @staticmethod
    def _join_item_key(*all_items):
        ret = []
        for item in all_items:
            if type(item) in (list, tuple):
                ret.extend(item)
            else:
                ret.append(item)
        return tuple(ret)

    def _slice_get(self, item, slice_idx, include_child=True, include_value=True):
        slice_list = []
        prev_slice_idx = 0
        for cur_slice_idx in slice_idx:
            slice_list.append(item[prev_slice_idx:cur_slice_idx])
            slice_list.append(slice(None))
            prev_slice_idx = cur_slice_idx + 1
        if prev_slice_idx < len(item):
            slice_list.append(item[prev_slice_idx:])

        child_q = [((), slice_list, self)]
        while child_q:
            pre_k, cur_slice_list, child = child_q.pop(0)
            cur_k, next_slice_list = cur_slice_list[0], cur_slice_list[1:]
            is_final = len(next_slice_list) == 0
            if not is_final:
                kwargs = dict(include_child=True, include_value=False)
            else:
                kwargs = dict(include_child=include_child, include_value=include_value)

            if not isinstance(cur_k, slice):
                sub_child = child.get(cur_k, create_child=False, **kwargs)
                if sub_child is None:
                    continue
                pre_k = self._join_item_key(pre_k, cur_k)
                if not is_final:
                    child_q.append((pre_k, next_slice_list, sub_child))
                else:
                    yield pre_k, sub_child
            else:
                children = child.items(**kwargs)
                if not is_final:
                    for sub_k, sub_sub_child in children:
                        child_q.append((self._join_item_key(pre_k, sub_k), next_slice_list, sub_sub_child))
                else:
                    for sub_k, sub_sub_child in children:
                        yield self._join_item_key(pre_k, sub_k), sub_sub_child

    def _internal_get_item(self, item):
        if type(item) not in (list, tuple):
            item = (item,)
        if len(item) == 0:
            return self
        create_child = False
        include_child = True
        include_value = True
        if item[-1] is Ellipsis:
            item = item[:-1]
            if len(item) == 0:
                return self
            create_child = True
            include_value = False
        if Ellipsis in item:
            raise NDFSKeyError(self, item, NDFSKeyError.KeyErrorType.ELLIPSIS)
        slice_idx = [i for i, k in enumerate(item) if isinstance(k, slice)]
        if len(slice_idx) == 0:
            return self._internal_get_cached(item, None, True, include_child, include_value, create_child)
        else:
            return self._slice_get(item, slice_idx, include_child, include_value)

    def _internal_put(self, item, value, append=False):
        if not self.writable:
            raise NDFSAccessViolation(self, item)

        if isinstance(value, self.__class__):
            raise ValueError(f"Cannot store a {self.__class__.__name__} object.")

        self._internal_verify_item(item)

        item_path = self.key_path(item)
        if os.path.isdir(item_path):
            raise NDFSLookupError(self, item, NDFSLookupError.LookupErrorType.SET_VALUE_OVER_CHILD)

        dir_path = os.path.dirname(item_path)
        os.makedirs(dir_path, exist_ok=True)

        if item_path in self.cache:
            del self.cache[item_path]
        return self._internal_write(item_path, value, append=append)

    def _internal_delete(self, item, ignore_errors=False):
        if not self.writable:
            raise NDFSAccessViolation(self, item)

        cur_path = self.key_path(item)
        if os.path.isdir(cur_path):
            shutil.rmtree(cur_path)
        elif os.path.isfile(cur_path):
            os.remove(cur_path)
        elif not ignore_errors:
            raise NDFSKeyError(self, item, NDFSKeyError.KeyErrorType.NO_SUCH_KEY)

    def _internal_copy_move(self, src, dst, move=True):
        if not self.writable:
            raise NDFSAccessViolation(self, dst)

        self._internal_verify_item(src)
        self._internal_verify_item(dst)
        src_path = self.key_path(src)
        dst_path = self.key_path(dst)

        if not os.path.exists(src_path):
            raise NDFSKeyError(self, src, NDFSKeyError.KeyErrorType.NO_SUCH_KEY)

        copy_file = os.path.isfile(src_path)
        if copy_file:
            if os.path.isdir(dst_path):
                raise NDFSLookupError(self, dst, NDFSLookupError.LookupErrorType.SET_VALUE_OVER_CHILD)
        else:
            if os.path.isfile(dst_path):
                raise NDFSLookupError(self, dst, NDFSLookupError.LookupErrorType.SET_CHILD_OVER_VALUE)
            elif os.path.isdir(dst_path):
                if len(os.listdir(dst_path)) > 0:
                    raise NDFSLookupError(self, dst, NDFSLookupError.LookupErrorType.SET_CHILD_OVER_CHILD)
                else:
                    shutil.rmtree(dst_path)

        dst_dir_path = os.path.dirname(dst_path)
        os.makedirs(dst_dir_path, exist_ok=True)
        if move:
            shutil.move(src_path, dst_path)
        elif copy_file:
            shutil.copy(src_path, dst_path)
        else:
            shutil.copytree(src_path, dst_path)

    def _internal_list_dir(self):
        if os.path.isdir(self.data_path):
            return sorted(os.listdir(self.data_path))
        else:
            return []

    def _internal_keys_paths(self):
        for k in self._internal_list_dir():
            yield k, self.key_path(k)

    ######################################################################################################
    # Explicit dict like interface
    ######################################################################################################

    def len(self):
        return len(self._internal_list_dir())

    def empty(self):
        return self.len() == 0

    def keys(self, include_child=True, include_value=True):
        for k, path in self._internal_keys_paths():
            if (include_value and os.path.isfile(path)) or (include_child and os.path.isdir(path)):
                yield k

    def value_keys(self):
        yield from self.keys(include_child=False, include_value=True)

    def child_keys(self):
        yield from self.keys(include_child=True, include_value=False)

    def items(self, include_child=True, include_value=True):
        for k in self.keys(include_child, include_value):
            yield k, self.get(k)

    def child_items(self):
        return self.items(include_child=True, include_value=False)

    def value_items(self):
        return self.items(include_child=False, include_value=True)

    def get(self, item, default_value=None, include_child=True, include_value=True, create_child=False):
        return self._internal_get_cached(item, default_value, False, include_child, include_value, create_child)

    def get_child(self, item):
        return self._internal_get_cached(item, include_child=True, include_value=False, create_child=True)

    def get_value(self, item):
        return self._internal_get_cached(item, include_child=False, include_value=True, create_child=False)

    def put(self, item, value):
        return self._internal_put(item, value)

    def append(self, item, value):
        return self._internal_put(item, value, append=True)

    def delete(self, item, ignore_errors=False):
        return self._internal_delete(item, ignore_errors=ignore_errors)

    def move(self, src, dst):
        return self._internal_copy_move(src, dst, move=True)

    def copy(self, src, dst):
        return self._internal_copy_move(src, dst, move=False)

    def child_exists(self, item):
        cur_path = self.key_path(item)
        return os.path.isdir(cur_path)

    def value_exists(self, item):
        cur_path = self.key_path(item)
        return os.path.isfile(cur_path)

    def exists(self, item):
        cur_path = self.key_path(item)
        return os.path.exists(cur_path)

    def clear_cache(self):
        self.cache.clear()
        gc.collect()

    ######################################################################################################
    # Implicit dict interface
    ######################################################################################################

    def __getitem__(self, item):
        return self._internal_get_item(item)

    def __setitem__(self, item, value):
        return self.put(item, value)

    def __delitem__(self, item):
        return self.delete(item, ignore_errors=False)

    def __contains__(self, item):
        return self.exists(item)

    def __iter__(self):
        return self.keys()

    def __len__(self):
        return self.len()
