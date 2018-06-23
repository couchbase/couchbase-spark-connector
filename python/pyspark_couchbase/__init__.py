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
from pyspark_couchbase.context import monkey_patch_context
from pyspark_couchbase.rdd import monkey_patch_rdd


def monkey_patch_all():
    monkey_patch_context()
    monkey_patch_rdd()


monkey_patch_all()
