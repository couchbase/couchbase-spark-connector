#
# Copyright (c) 2015 Couchbase, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
Utilities for using scala api
"""

def _load_class(ctx, name):
    return ctx._jvm.java.lang.Thread.currentThread() \
        .getContextClassLoader().loadClass(name)


def _get_java_array(gateway, java_type, iterable):
    java_type = gateway.jvm.__getattr__(java_type)
    lst = list(iterable)
    arr = gateway.new_array(java_type, len(lst))
    for i, e in enumerate(lst):
        arr[i] = e
    return arr


def _scala_api(ctx):
    if not hasattr(_scala_api, "instance"):
        _scala_api.instance = _load_class(
            ctx, "com.couchbase.spark.api.python.PythonAPI").newInstance()
    return _scala_api.instance
