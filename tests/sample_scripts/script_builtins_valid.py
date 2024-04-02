# Exercise allowed valid builtins & modules

# modules declared on constants.AGENT_SCRIPT_BUILTIN_MODULES
import abc
import base64
import binascii
import collections
import copy
import datetime
import decimal
import enum
import functools
import gzip
import hashlib
import ipaddress
import itertools
import json
import math
import operator
import random
import re
import string
import struct
import time
import types
import typing
import uuid
import zlib

# Exercise from import to check it's well resolved
from typing import Dict


# Class definitions
class Foo:
    def __init__(self):
        self.bar = "bar_value"

    @staticmethod
    def static_bar(value: typing.List[str] = None) -> Dict:
        return {x: x for x in value}

    @classmethod
    def class_bar(cls):
        return "class_bar"

    @property
    def property_bar(self):
        return "property_bar"


# Allowed valid builtins
sum([1])
min([1])
max([1])
abs(-1)
round(1.1)
pow(2, 3)
len([1])
all([1])
any([1])
map(lambda x: x, [1])
filter(lambda x: x, [1])
iter([1])
enumerate([1])
zip([1], [1])
sorted([1])
reversed([1])
hash(1)
slice(1)
next(iter([1]))
list(range(10))
dict(a=1, b=2)
str(1)
int("1")
float(1)
bool(1)
set([1, 2, 3])
tuple([1, 2, 3])


# class operators
assert hasattr(Foo(), "bar")
assert getattr(Foo(), "bar") == "bar_value"
assert dir(Foo()) == [
    "__class__",
    "__delattr__",
    "__dict__",
    "__dir__",
    "__doc__",
    "__eq__",
    "__format__",
    "__ge__",
    "__getattribute__",
    "__getstate__",
    "__gt__",
    "__hash__",
    "__init__",
    "__init_subclass__",
    "__le__",
    "__lt__",
    "__module__",
    "__ne__",
    "__new__",
    "__reduce__",
    "__reduce_ex__",
    "__repr__",
    "__setattr__",
    "__sizeof__",
    "__str__",
    "__subclasshook__",
    "__weakref__",
    "bar",
    "class_bar",
    "property_bar",
    "static_bar",
]
assert Foo().property_bar == "property_bar"


# getattr/setattr
foo = Foo()
foo.bar = "bar2"
assert foo.bar == "bar2"


# getitem/setitem/unpacking
di = {"a": 1}
di["a"] = 2
assert di["a"] == 2
li = [1, 2]
li[0] = 1
assert li[0] == 1
assert [1][0] == 1
assert (1,)[0] == 1
a, b = li
a, b = tuple(li)


# Exercise valid modules
abc.ABC
base64.b64encode(b"hello")
binascii.hexlify(b"hello")
collections.defaultdict()
copy.deepcopy([1])
datetime.datetime.now()
decimal.Decimal("1.1")
enum.Enum("Color", "RED GREEN BLUE")
functools.partial(lambda x: x, 1)
gzip.compress(b"hello")
hashlib.sha256(b"hello")
itertools.combinations([1, 2, 3], 2)
ipaddress.ip_address("1.1.1.1")
json.dumps({"foo": "bar"})
math.ceil(1.1)
operator.add(1, 2)
random.choice([1, 2, 3])
re.compile("foo")
re.compile("foo")
string.ascii_letters
struct.pack("i", 1)
time.time()
types.SimpleNamespace(foo="bar")
uuid.uuid4()
zlib.compress(b"hello")


# Dataclasses do not work as AnnAssign is forbidden in RestrictedPython AST
# this prevents us from adding type annotations to the class properties
# causing dataclasses to fail
#
# @dataclasses.dataclass
# class DataClass:
#     foo: <type> is not supported as AnnAssign is forbidden in RestrictedPython AST
#     foo = dataclasses.field(default='foo')
#     bar: str = dataclasses.field(default='bar')


# iteration constructs
for x in [1, 2, 3]:
    pass

for x in [x for x in [1, 2, 3]]:
    pass

a = 0
while a < 1:
    a = a + 1

my_str = "{x}"
assert f"{my_str}" == "{x}"
assert my_str.format(x="x") == "x"

str.format_map("Hello {foo.__dict__}", {"foo": str})


def execute_script_handler(*args, **kwargs):
    return "all is good"


# ensure we have a replacement for _apply_
f = {"b": "b_v"}


def foo(a, b=None):
    return a + b


assert foo("1", **f) == "1b_v"
