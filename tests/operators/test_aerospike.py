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

import unittest
from unittest.mock import patch, Mock
from aerospike_provider.operators.aerospike import AerospikeGetKeyOperator, AerospikePutKeyOperator
import aerospike

class TestAerospikeGetKeyOperator(unittest.TestCase):
    def setUp(self):
        self.namespace = 'test_namespace'
        self.set = 'test_set'
        self.key = 'test_key'
        self.policy = { aerospike.POLICY_KEY_SEND }
        self.task_id = 'test_task'
        self.metadata = {'ttl': 1000, 'gen': 4}
        self.bins = {'name': 'Aerospike Test', 'version': "1.0.0"}

        self.operator = AerospikeGetKeyOperator(
            namespace=self.namespace,
            set=self.set,
            key=self.key,
            policy=self.policy,
            task_id=self.task_id
        )

    @patch('aerospike_provider.hooks.aerospike.AerospikeHook.get_conn')
    def test_execute(self, mock_hock_conn):
        mock_hock_conn.return_value = Mock()
        self.operator.parse_records = Mock()
        self.operator.parse_records.return_value = [1]
        self.operator.execute({})

        mock_hock_conn.return_value.get_record.assert_called_once_with(
            namespace='test_namespace',
            set='test_set',
            key='test_key',
            policy={ aerospike.POLICY_KEY_SEND }
        )

    def test_parse_records_as_tuple(self):
        mock = ( (self.namespace, self.set, self.key), self.metadata, self.bins)
        mock_parsed = self.operator.parse_records(records=mock)
        expected = [{"namespace": self.namespace, "set": self.set, "key": self.key, "metadata": self.metadata, "bins": self.bins}]
        assert mock_parsed == expected


    def test_parse_records_as_list(self):
        mock = [( (self.namespace, self.set, self.key), self.metadata, self.bins), ( (self.namespace, self.set, self.key), self.metadata, self.bins)]
        mock_parsed = self.operator.parse_records(records=mock)

        expected = [
            {"namespace": self.namespace, "set": self.set, "key": self.key, "metadata": self.metadata, "bins": self.bins},
            {"namespace": self.namespace, "set": self.set, "key": self.key, "metadata": self.metadata, "bins": self.bins}
            ]
        assert mock_parsed == expected


    def test_parse_records_as_exception(self):
        mock = {}
        with self.assertRaises(ValueError):
            self.operator.parse_records(records=mock)


    def test_create_dict_from_record_with_bins(self):
        mock = ( (self.namespace, self.set, self.key), self.metadata, self.bins)
        mock_result = self.operator.create_dict_from_record(record=mock)

        expected = {"namespace": self.namespace, "set": self.set, "key": self.key, "metadata": self.metadata, "bins": self.bins}
        assert mock_result == expected

    def test_create_dict_from_record_no_bins(self):
        mock = ( (self.namespace, self.set, self.key), self.metadata)
        mock_result = self.operator.create_dict_from_record(record=mock)

        expected = {"namespace": self.namespace, "set": self.set, "key": self.key, "metadata": self.metadata}
        assert mock_result == expected


class TestAerospikePutKeyOperator(unittest.TestCase):
    def setUp(self):
        self.operator = AerospikePutKeyOperator(
            namespace='test_namespace',
            set='test_set',
            key='test_key',
            bins={'bin1': 'value1'},
            metadata={'ttl': 1000},
            policy={'key': aerospike.POLICY_EXISTS_IGNORE},
            task_id='test_task'
        )

    @patch('aerospike_provider.hooks.aerospike.AerospikeHook.get_conn')
    def test_execute(self, mock_hock_conn):
        mock_hock_conn.return_value = Mock()
        self.operator.execute({})

        mock_hock_conn.return_value.put.assert_called_once_with(
            namespace='test_namespace',
            set='test_set',
            key='test_key',
            bins={'bin1': 'value1'},
            metadata={'ttl': 1000},
            policy={'key': aerospike.POLICY_EXISTS_IGNORE}
        )
