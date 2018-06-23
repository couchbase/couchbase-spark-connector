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
import json


class RawJsonDocument(object):
    """
    Represents a document which is already json encoded.
    """

    def __init__(self, id, content, cas=0, expiry=0,
                 mutation_token=None):
        """
        Creates a document from already encoded content

        Parameters
        ----------
        id: string
            per bucket unique document id
        content: string
            json encoded contents
        cas: int
            the CAS value
        expiry: int
            expiration time of the document
        mutation_token: pypsark_couchbase.types.MutationToken
            mutation token of the document
        """
        self.id = id
        self.content = content
        self.cas = cas
        self.expiry = expiry
        self.mutation_token = mutation_token

    def __getitem__(self, key):
        return getattr(self, key)

    def to_dict(self):
        return vars(self)

    def __reduce__(self):
        return (
            RawJsonDocument, (
                self.id, self.content,
                self.cas, self.expiry, self.mutation_token))

    def __repr__(self):
        return str(self.to_dict())


def _build_subdoclookup_value(value):
    """
    Helper function to decode json objects.
    Java null is mappped to python None in Pyrolite. Therefore
    we may get None objects if the query does not return
    any results, thus requiring special checking while decoding.
    """
    return json.loads(value) if value is not None else None


def _create_subdoclookup_result(
        id, keys, values, cas, exists):
    """
    Factory function used by scala pickler to create
    SubdocLookupResult instances.

    Parameters
    ----------
    id: string
        Doc id
    keys: List[string]
        List of subdoclookup query ids
    values: List[string]
        List of json encoded subdoclookup
        query results. Decoded to dict
        with json
    exists: Dict[string: bool]
        Results of exists query
    """
    content = {
        k: _build_subdoclookup_value(v)
            for k, v in zip(keys, values)}
    return SubdocLookupResult(
        id, content, cas, exists)


class SubdocLookupResult(object):
    """
    Used for returning results of subdoclookup query
    """

    def __init__(self, id, content, cas, exists):
        """
        Parameters
        ----------
        id: string
            Per bucket unique document id
        content: Dict[str: Dict]
            Subdoclookup results, which maps
            the subdoclookup query ids to objects
            which are json decoded
        cas: int
            The CAS value
        exists: Dict[str: bool]
            Result of the 'exists' query, mapping
            ids to results
        """
        self.id = id
        self.content = content
        self.cas = cas
        self.exists = exists

    def __getitem__(self, key):
        return getattr(self, key)

    def to_dict(self):
        return vars(self)

    def __repr__(self):
        return str(self.to_dict())


class MutationToken(object):

    def __init__(
            self, vbucket_id, vbucket_uuid,
            sequence_number, bucket):
        self.vbucket_id = vbucket_id
        self.vbucket_uuid = vbucket_uuid
        self.sequence_number = sequence_number
        self.bucket = bucket

    def __reduce(self):
        return (
            MutationToken, (
                self.vbucket_id,
                self.vbucket_uuid, self.sequence_number,
                self.bucket))

    def __repr__(self):
        return str(vars(self))
