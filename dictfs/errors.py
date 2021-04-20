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
from enum import Enum, auto


class NDException(Exception):
    def __init__(self, obj, item, msg):
        self.obj = obj
        self.item = item
        super().__init__(msg)


class NDAccessViolation(NDException):
    def __init__(self, obj, item):
        msg = f"{obj} was opened in read-only mode when trying to modify {item}."
        super().__init__(obj, item, msg)


class NDLookupError(NDException):
    class Type(Enum):
        NOT_INCLUDE_CHILD = auto()
        NOT_INCLUDE_DATA = auto()
        DATA_SUB_ITEM = auto()
        SET_DATA_OVER_CHILD = auto()
        SET_CHILD_OVER_DATA = auto()
        SET_CHILD_OVER_CHILD = auto()

    def __init__(self, obj, error: Type, item, sub_item=None):
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


class NDKeyError(NDException):
    class Type(Enum):
        NO_SUCH_KEY = auto()
        INVALID_KEY = auto()
        NO_SEARCH_TERM = auto()

    def __init__(self, obj, error: Type, item):
        msg = f"Key error in {obj} with item {item}."
        if error == self.Type.NO_SUCH_KEY:
            msg = f"Item {item} does not exist in {obj}."
        elif error == self.Type.INVALID_KEY:
            msg = f"Invalid key: {item}."
        elif error == self.Type.NO_SEARCH_TERM:
            msg = "Only search terms may use ellipsis (...), slice (:) or a pattern (regular-expression)."

        super().__init__(obj, item, msg)
        self.error = error
