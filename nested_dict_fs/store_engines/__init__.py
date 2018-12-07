"""
Author: Liran Funaro <funaro@cs.technion.ac.il>

Copyright (C) 2006-2018 Liran Funaro

This program is free software; you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the Free
Software Foundation; either version 2 of the License, or (at your option)
any later version.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
more details.

You should have received a copy of the GNU General Public License along
with this program; if not, write to the Free Software Foundation, Inc., 51
Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
"""

DEFAULT_STORE_ENGINE = 'msgpack-numpy'


def get_store_engine(method):
    if type(method) in (list, tuple) and len(method) == 2:
        return method
    elif not isinstance(method, str):
        raise ValueError(f"Store engine must be a string or a tuple of (write, read) methods. Not {type(method)}.")

    if 'msgpack' in method:
        from nested_dict_fs.store_engines import msgpack
        if 'numpy' in method:
            import msgpack_numpy as m
            m.patch()
        return msgpack.write, msgpack.read
    else:
        raise ValueError(f"No such storage engine: {method}")
