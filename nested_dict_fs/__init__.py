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
from collections import deque

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
    class Type(Enum):
        NOT_INCLUDE_CHILD = auto()
        NOT_INCLUDE_DATA = auto()
        DATA_SUB_ITEM = auto()
        SET_DATA_OVER_CHILD = auto()
        SET_CHILD_OVER_DATA = auto()
        SET_CHILD_OVER_CHILD = auto()

    def __init__(self, obj, item, error: Type, sub_item=None):
        msg = f"Lookup error in {obj} with item {item}."
        if error == self.Type.NOT_INCLUDE_CHILD:
            msg = f"Item {item} is a child but children were not included in the search."
        elif error == self.Type.NOT_INCLUDE_DATA:
            msg = f"Item {item} is data but data were not included in the search."
        elif error == self.Type.DATA_SUB_ITEM:
            msg = f"Sub item {sub_item} is a data but requested its child {item}."
        elif error == self.Type.SET_DATA_OVER_CHILD:
            msg = f"Cannot set/copy/move data to an existing child. Must remove subtree {item}."
        elif error == self.Type.SET_CHILD_OVER_DATA:
            msg = f"Cannot copy/move a child to an existing data. Must remove data {item}."
        elif error == self.Type.SET_CHILD_OVER_CHILD:
            msg = f"Cannot copy/move a child to an existing non empty child. Must remove subtree {item}."

        super().__init__(obj, item, msg)
        self.error = error
        self.sub_item = sub_item


class NDFSKeyError(NDFSException):
    class Type(Enum):
        NO_SUCH_KEY = auto()
        INVALID_KEY = auto()
        ELLIPSIS = auto()
        SLICE = auto()

    def __init__(self, obj, item, error: Type):
        msg = f"Key error in {obj} with item {item}."
        if error == self.Type.NO_SUCH_KEY:
            msg = f"Item {item} does not exist in {obj}."
        elif error == self.Type.INVALID_KEY:
            msg = f"Invalid key: {item}."
        elif error == self.Type.ELLIPSIS:
            msg = "Ellipsis (...) may not be used as a key."
        elif error == self.Type.SLICE:
            msg = "Slice (:) may not be used as a key."

        super().__init__(obj, item, msg)
        self.error = error


class NestedDictFS:
    __slots__ = ('data_path', 'mode', 'writable', 'store_engine', 'write_method', 'read_method', 'compress_level',
                 'cache')

    def __init__(self, data_path, mode='r', cache_size=None, shared_cache=None,
                 store_engine=store_engines.DEFAULT_STORE_ENGINE, compress_level=9):
        """
        :param data_path: The key value store data path. It is possible to start from sub folder of existing path.
            Might be a string represents the path or an existing NestedDictFS object.
        :param mode: Should be 'r' for read only, 'w', 'rw' or 'wr' for writable,
            and 'c' for allow creation of a new data path (lazy).
        :param cache_size: Define the cache size to use. Defaults to 128.
        :param shared_cache: Can be used to pass a cache object from another instance to be shared (ignores cache_size).
        :param store_engine: The class does not infer the store engine from the data path, so it is
            up to the programmer to know the store engine used for an existing data path.
            Use one of the following:
            - plain: convert any object plain text (utf-8).
            - binary: attempt to write any object directly to a file.
            - pickle: python default pickle implementation.
            - msgpack: uses msgpack to store objects.
            - msgpack-numpy (default): uses msgpack with msgpack_numpy to also support numpy arrays.
        :param compress_level: See `gzip`.
        """
        if isinstance(data_path, self.__class__):
            parent = data_path
            data_path = parent.data_path
            shared_cache = parent.cache
            store_engine = parent.store_engine
            compress_level = parent.compress_level
        if not isinstance(data_path, str):
            raise TypeError(
                f"data_path must be a string or a {self.__class__.__name__} object. Not a {type(data_path)}.")

        self.data_path = self._normalize_path(data_path)
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

    def key_path(self, item):
        """ Returns the data path of an item """
        item = self._internal_verify_item(item)
        return self._unsafe_key_path(item)

    def path_key(self, path):
        """ Returns the key of a data path """
        path = self._normalize_path(path)
        if path == self.data_path:
            return ()
        common = os.path.commonpath([self.data_path, path])
        if common != self.data_path:
            raise ValueError("Sub path must be in the current path subtree.")
        item = self._unsafe_path_key(path)
        if len(item) == 1:
            return item[0]
        else:
            return item

    ######################################################################################################
    # Internal helpers
    ######################################################################################################

    @staticmethod
    def _normalize_path(path):
        """
        realpath: Return the canonical path of the specified filename, eliminating any symbolic links encountered in
            the path (if they are supported by the operating system).
        normcase: Normalize the case of a pathname. On Unix and Mac OS X, this returns the path unchanged;
            on case-insensitive filesystems, it converts the path to lowercase.
            On Windows, it also converts forward slashes to backward slashes.
        abspath: Return a normalized absolutized version of the pathname path.
        """
        return os.path.abspath(os.path.normcase(os.path.realpath(path)))

    def _unsafe_key_path(self, item):
        return os.path.join(self.data_path, *item)

    def _unsafe_path_key(self, path):
        sub_path = os.path.relpath(path, self.data_path)
        return tuple(sub_path.split(os.path.sep))

    def _internal_list_dir(self):
        if os.path.isdir(self.data_path):
            return sorted(os.listdir(self.data_path))
        else:
            return []

    def _internal_keys_paths(self):
        for k in self._internal_list_dir():
            yield k, self._unsafe_key_path((k,))

    @staticmethod
    def _internal_path_exists(path, include_child=True, include_data=True):
        return (include_data and os.path.isfile(path)) or (include_child and os.path.isdir(path))

    def _internal_keys(self, include_child=True, include_data=True):
        for k, path in self._internal_keys_paths():
            if self._internal_path_exists(path, include_child, include_data):
                yield k, path

    def _internal_open(self, filepath, mode='rb'):
        if self.compress_level == 0:
            return open(filepath, mode)
        else:
            return gzip.open(filepath, mode, compresslevel=self.compress_level)

    def _internal_write(self, filepath, obj, append=False):
        with self._internal_open(filepath, 'ab' if append else 'wb') as f:
            self.write_method(f, obj)

    def _internal_read(self, filepath):
        with self._internal_open(filepath, 'rb') as f:
            return self.read_method(f)

    def _internal_get_child(self, item_path, create=False):
        mode = self.mode if not create else 'c'
        return self.__class__(item_path, mode=mode, shared_cache=self.cache, store_engine=self.store_engine)

    def _internal_verify_item(self, item, is_search_key=False):
        if type(item) not in (list, tuple):
            item = (item,)
        if Ellipsis in item:
            raise NDFSKeyError(self, item, NDFSKeyError.Type.ELLIPSIS)
        if not is_search_key and any(isinstance(k, slice) for k in item):
            raise NDFSKeyError(self, item, NDFSKeyError.Type.SLICE)

        item = tuple([str(k) if not isinstance(k, slice) else k for k in item])
        for k in item:
            if isinstance(k, slice):
                continue
            if k in ('.', '..') or os.path.sep in k:
                raise NDFSKeyError(self, item, NDFSKeyError.Type.INVALID_KEY)

        if is_search_key:
            return item

        for i in range(1, len(item)):
            sub_item = item[:i]
            sub_item_path = self._unsafe_key_path(sub_item)
            if os.path.isfile(sub_item_path):
                raise NDFSLookupError(self, item, NDFSLookupError.Type.DATA_SUB_ITEM, sub_item)

        return item

    def _internal_get_direct(self, item, item_path, default_value=None, raise_err=True,
                             include_child=True, include_data=True, create_child=False):
        include_child |= create_child

        if os.path.isdir(item_path):
            if not include_child:
                raise NDFSLookupError(self, item, NDFSLookupError.Type.NOT_INCLUDE_CHILD)
            return self._internal_get_child(item_path, create=False)

        if os.path.isfile(item_path):
            if not include_data:
                raise NDFSLookupError(self, item, NDFSLookupError.Type.NOT_INCLUDE_DATA)
            return self._internal_read(item_path)

        if create_child:
            if self.writable:
                return self._internal_get_child(item_path, create=True)
            else:
                raise NDFSAccessViolation(self, item)
        elif raise_err:
            raise NDFSKeyError(self, item, NDFSKeyError.Type.NO_SUCH_KEY)

        return default_value

    def _internal_get_cached(self, item, item_path, default_value=None, raise_err=True,
                             include_child=True, include_value=True, create_child=False):
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
            raise NDFSLookupError(self, item, NDFSLookupError.Type.NOT_INCLUDE_CHILD)
        elif not is_child and not include_value and ret_value is not default_value:
            raise NDFSLookupError(self, item, NDFSLookupError.Type.NOT_INCLUDE_DATA)

        return ret_value

    ######################################################################################################
    # Specialization GET methods
    ######################################################################################################

    def get_direct(self, item, default_value=None, raise_err=True,
                   include_child=True, include_data=True, create_child=False):
        """ Get an item directly (skipping the cache) """
        item_path = self.key_path(item)
        if item_path == self.data_path:
            return self
        include_child |= create_child
        return self._internal_get_direct(item, item_path, default_value, raise_err, include_child, include_data,
                                         create_child)

    def get_cached(self, item, default_value=None, raise_err=True,
                   include_child=True, include_data=True, create_child=False):
        """ Get an item. If it is already in the cache, than a cached item will be returned. """
        item_path = self.key_path(item)
        if item_path == self.data_path:
            return self
        return self._internal_get_cached(item, item_path, default_value, raise_err, include_child, include_data,
                                         create_child)

    ######################################################################################################
    # Search methods
    ######################################################################################################

    @staticmethod
    def _join_item_key(*all_items):
        ret = []
        for item in all_items:
            if type(item) in (list, tuple):
                ret.extend(item)
            else:
                ret.append(item)
        return tuple(ret)

    @staticmethod
    def _split_list_by_type(lst, *split_types):
        items_type = ((i, k, type(k)) for i, k in enumerate(lst))
        type_idx = [(i, k, t) for i, k, t in items_type if t in split_types]

        ret_lst = []
        prev_idx = 0
        for i, k, t in type_idx:
            ret_lst.append(lst[prev_idx:i])
            ret_lst.append(t)
            prev_idx = i + 1
        if prev_idx < len(lst):
            ret_lst.append(lst[prev_idx:])

        return ret_lst

    def _yield_item(self, item, item_path, yield_keys=True, yield_values=True):
        if len(item) == 1:
            item = item[0]
        if not yield_values:
            return item

        ret_val = self._internal_get_cached(item, item_path)
        if yield_keys:
            return item, ret_val
        else:
            return ret_val

    def _internal_walk(self, include_child=True, include_data=True, topdown=True):
        for root, dirs, files in os.walk(self.data_path, topdown=topdown):
            iter_items = []
            if include_child:
                iter_items.extend(dirs)
            if include_data:
                iter_items.extend(files)

            for item_name in iter_items:
                item_path = os.path.join(root, item_name)
                item = self._unsafe_path_key(item_path)
                yield item, item_path

    def walk(self, include_child=True, include_data=True, yield_keys=True, yield_values=True, topdown=True):
        """ Walk the current item subtree and yields key/value, key or value """
        for item, item_path in self._internal_walk(include_child, include_data, topdown):
            yield self._yield_item(item, item_path, yield_keys, yield_values)

    def search(self, item, include_child=True, include_data=True, yield_keys=True, yield_values=True):
        """ Search the current sub tree """
        item = self._internal_verify_item(item, is_search_key=True)
        if len(item) == 0:
            raise ValueError("Search term must be with at least one criteria.")
        slice_list = self._split_list_by_type(item, slice)

        child_q = deque()
        child_q.append(((), slice_list))
        while child_q:
            pre_k, cur_slice_list = child_q.popleft()
            cur_k, next_slice_list = cur_slice_list[0], cur_slice_list[1:]
            is_final = len(next_slice_list) == 0

            if cur_k != slice:
                joined_k = self._join_item_key(pre_k, cur_k)
                cur_path = self._unsafe_key_path(joined_k)
                if is_final:
                    if self._internal_path_exists(cur_path, include_child, include_data):
                        yield self._yield_item(joined_k, cur_path, yield_keys, yield_values)
                elif os.path.isdir(cur_path):
                    child_q.append((joined_k, next_slice_list))
            else:
                child = self.get_child(pre_k)
                if not is_final:
                    for sub_k, _ in child._internal_keys(include_child=True, include_data=False):
                        joined_k = self._join_item_key(pre_k, sub_k)
                        child_q.append((joined_k, next_slice_list))
                else:
                    for sub_k, _ in child._internal_keys(include_child=include_child, include_data=include_data):
                        joined_k = self._join_item_key(pre_k, sub_k)
                        cur_path = self._unsafe_key_path(joined_k)
                        yield self._yield_item(joined_k, cur_path, yield_keys, yield_values)

    ######################################################################################################
    # Internal modifiers
    ######################################################################################################

    def _internal_put(self, item, value, append=False):
        if not self.writable:
            raise NDFSAccessViolation(self, item)

        if isinstance(value, self.__class__):
            raise ValueError(f"Cannot store a {self.__class__.__name__} object.")

        item_path = self.key_path(item)
        if os.path.isdir(item_path):
            raise NDFSLookupError(self, item, NDFSLookupError.Type.SET_DATA_OVER_CHILD)

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
            raise NDFSKeyError(self, item, NDFSKeyError.Type.NO_SUCH_KEY)

    def _internal_copy_move(self, src, dst, move=True):
        if not self.writable:
            raise NDFSAccessViolation(self, dst)

        src_path = self.key_path(src)
        dst_path = self.key_path(dst)

        if not os.path.exists(src_path):
            raise NDFSKeyError(self, src, NDFSKeyError.Type.NO_SUCH_KEY)

        copy_file = os.path.isfile(src_path)
        if copy_file:
            if os.path.isdir(dst_path):
                raise NDFSLookupError(self, dst, NDFSLookupError.Type.SET_DATA_OVER_CHILD)
        else:
            if os.path.isfile(dst_path):
                raise NDFSLookupError(self, dst, NDFSLookupError.Type.SET_CHILD_OVER_DATA)
            elif os.path.isdir(dst_path):
                if len(os.listdir(dst_path)) > 0:
                    raise NDFSLookupError(self, dst, NDFSLookupError.Type.SET_CHILD_OVER_CHILD)
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

    ######################################################################################################
    # Explicit dict like interface
    ######################################################################################################

    def len(self):
        return len(self._internal_list_dir())

    def empty(self):
        return self.len() == 0

    @property
    def keys(self):
        return NestedDictIterator(self, include_child=True, include_data=True,
                                  yield_keys=True, yield_values=False)

    @property
    def data_keys(self):
        return NestedDictIterator(self, include_child=False, include_data=True,
                                  yield_keys=True, yield_values=False)

    @property
    def child_keys(self):
        return NestedDictIterator(self, include_child=True, include_data=False,
                                  yield_keys=True, yield_values=False)

    @property
    def values(self):
        return NestedDictIterator(self, include_child=True, include_data=True,
                                  yield_keys=False, yield_values=True)

    @property
    def data_values(self):
        return NestedDictIterator(self, include_child=False, include_data=True,
                                  yield_keys=False, yield_values=True)

    @property
    def child_values(self):
        return NestedDictIterator(self, include_child=True, include_data=False,
                                  yield_keys=False, yield_values=True)

    @property
    def items(self):
        return NestedDictIterator(self, include_child=True, include_data=True,
                                  yield_keys=True, yield_values=True)

    @property
    def child_items(self):
        return NestedDictIterator(self, include_child=True, include_data=False,
                                  yield_keys=True, yield_values=True)

    @property
    def data_items(self):
        return NestedDictIterator(self, include_child=False, include_data=True,
                                  yield_keys=True, yield_values=True)

    def get(self, item, default_value=None, include_child=True, include_data=True, create_child=False):
        return self.get_cached(item, default_value, False, include_child, include_data, create_child)

    def get_child(self, item):
        return self.get_cached(item, include_child=True, include_data=False, create_child=True)

    def get_data(self, item):
        return self.get_cached(item, include_child=False, include_data=True, create_child=False)

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

    def update(self, input_dict, max_depth=0):
        if not isinstance(input_dict, dict):
            raise ValueError("Input must be a dict.")

        if max_depth < 1:
            for k, v in input_dict.items():
                self.put(k, v)
        else:
            for k, v in input_dict.items():
                if isinstance(v, dict):
                    self.get_child(k).update(v, max_depth-1)
                else:
                    self.put(k, v)

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
        return self.get_cached(item, None, True, include_child=True, include_data=True, create_child=False)

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


class NestedDictIterator:
    __slots__ = 'owner', 'include_child', 'include_data', 'yield_keys', 'yield_values'

    def __init__(self, owner: NestedDictFS, include_child=True, include_data=True, yield_keys=True, yield_values=True):
        self.owner = owner
        self.include_child = include_child
        self.include_data = include_data
        self.yield_keys = yield_keys
        self.yield_values = yield_values

    def __getitem__(self, item):
        return self.owner.search(item, self.include_child, self.include_data, self.yield_keys, self.yield_values)

    def __call__(self, include_child=None, include_data=None):
        if include_child is None:
            include_child = self.include_child
        if include_data is None:
            include_data = self.include_data
        yield from self.owner.search(slice(None), include_child, include_data, self.yield_keys, self.yield_values)

    def __iter__(self):
        yield from self.owner.search(slice(None), self.include_child, self.include_data,
                                     self.yield_keys, self.yield_values)
