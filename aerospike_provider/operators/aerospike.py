#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence, Union, List, Dict, Any

if TYPE_CHECKING:
    from airflow.utils.context import Context

import aerospike
from aerospike_provider.hooks.aerospike import AerospikeHook
from airflow.models.baseoperator import BaseOperator


class AerospikePutKeyOperator(BaseOperator):
    """
    Create a new record, add or remove bins.

    This can also remove a record (if exists) using ` `{"bin": aerospuke.null() }`` if it's the last bin.

    :param key: key to save in the db.
    :param namespace: namespace to use in aerospike db
    :param set: set name in the namespace
    :param bins: bins name and data saved along with a key as key values. For example: `{"bin": value}`
    :param metadata: metadata about the key eg. ttl. For example: `{"ttl": 0}`
    :param policy: which policy the key should be saved with. default `POLICY_EXISTS_IGNORE`. ref: https://developer.aerospike.com/client/usage/atomic/update#policies
    :param aerospike_conn_id: aerospike connection to use, defaults to 'aerospike_default'
    """

    template_fields: Sequence[str] = ("key", "bins", "metadata", )
    template_ext: Sequence[str] = ()
    ui_color = "#66c3ff"

    def __init__(
        self,
        namespace: str,
        set: str,
        key: str,
        bins: dict,
        metadata: Union[dict, Any] = None,
        policy: Dict[str, Any] = {'key': aerospike.POLICY_EXISTS_IGNORE},
        aerospike_conn_id: str = "aerospike_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.key = key
        self.namespace = namespace
        self.set = set
        self.key = key
        self.bins = bins
        self.metadata = metadata
        self.policy = policy
        self.aerospike_conn_id = aerospike_conn_id

    def execute(self, context: Context) -> None:
        with AerospikeHook(self.aerospike_conn_id) as hook:
            self.log.info('Storing %s as key', self.key)
            hook.put(key=self.key, bins=self.bins, metadata=self.metadata, namespace=self.namespace, set=self.set, policy=self.policy)
            self.log.info('Stored key successfully')


class AerospikeGetKeyOperator(BaseOperator):
    """
    Read an existing record(s) metadata and all of its bins for a specified key.

    :param namespace: namespace to use in aerospike db
    :param set: set name in the namespace
    :param key: key to get and return. can be a single key or a list of keys
    :param policy: which policy the key should be saved with. default `POLICY_KEY_SEND`
    :param aerospike_conn_id: aerospike connection to use, defaults to 'aerospike_default'
    """

    template_fields: Sequence[str] = ("key",)
    template_ext: Sequence[str] = ()
    ui_color = "#66c3ff"

    def __init__(
        self,
        namespace: str,
        set: str,
        key: Union[List[str], str],
        policy: dict = {'key': aerospike.POLICY_KEY_SEND},
        aerospike_conn_id: str = "aerospike_default",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.key = key
        self.namespace = namespace
        self.set = set
        self.key = key
        self.policy = policy
        self.aerospike_conn_id = aerospike_conn_id

    def execute(self, context: Context) -> list:
        with AerospikeHook(self.aerospike_conn_id) as hook:
            self.log.info('Fetching key')
            records = hook.get_record(key=self.key, namespace=self.namespace, set=self.set, policy=self.policy)
            parsed_records = self.parse_records(records=records)
            self.log.info('Got %s records', len(parsed_records))
            return parsed_records

    def parse_records(self, records: Union[List, tuple]) -> list:
        # Removing the `bytearray` object from records since object of type bytearray is not JSON serializable for Xcom.
        if isinstance(records, list):
            data = list(map(self.create_dict_from_record, records))
        elif isinstance(records, tuple):
            data = [self.create_dict_from_record(record=records)]
        else:
            raise ValueError(f"Expecting 'list' or 'tuple', got: {type(records)}")
        return data

    @staticmethod
    def create_dict_from_record(record: tuple) -> dict:
        try:
            return {
                "namespace": record[0][0],
                "set": record[0][1],
                "key": record[0][2],
                "metadata": record[1],
                "bins": record[2]
            }
        except IndexError:
            # Handling an error when there are no 'bins' the data
            return {
                "namespace": record[0][0],
                "set": record[0][1],
                "key": record[0][2],
                "metadata": record[1]
            }
