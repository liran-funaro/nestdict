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
import sys
import pkgutil
import importlib


DEFAULT_STORE_ENGINE = 'msgpack-numpy'
ENGINES = list([modname for _, modname, _ in pkgutil.iter_modules(sys.modules[__name__].__path__)])


def get_store_engine(method):
    if type(method) in (list, tuple) and len(method) == 2:
        return method
    elif not isinstance(method, str):
        raise TypeError(f"Store engine must be a string or a tuple of (write, read) methods. Not {type(method)}.")

    if method not in ENGINES:
        raise ValueError(f"No such storage engine: {method}")

    eng = importlib.import_module(f'.{method}', __name__)
    return eng.write, eng.read

