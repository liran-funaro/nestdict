"""
Author: Liran Funaro <liran.funaro@gmail.com>

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
import shutil
import tempfile

from nesteddict import NestedDictFS, NDKeyError, NDLookupError, NDAccessViolation


def random_folder():
    return uuid.uuid4().hex.upper()[:8]


def clean(path):
    if os.path.isdir(path):
        shutil.rmtree(path)
    elif os.path.isfile(path):
        os.remove(path)


def setup_test():
    path = os.path.join(tempfile.gettempdir(), random_folder())
    clean(path)
    return path


def get_ret_list(ret_obj):
    ret = []
    for kv in ret_obj:
        if isinstance(kv, NestedDictFS):
            kv = kv.data_path
        ret.append(kv)
    return ret


def get_ret_list_items(ret_obj):
    ret = []
    for k, v in ret_obj:
        if isinstance(v, NestedDictFS):
            v = v.data_path
        ret.append((k, v))
    return ret


def get_keys(*keys):
    return [k if (isinstance(k, tuple) and len(k) != 1) or isinstance(k, str) else k[0] for k in keys]


def get_key_path(obj, *keys):
    keys = get_keys(*keys)
    return [(k, obj.key_path(k)) for k in keys]
