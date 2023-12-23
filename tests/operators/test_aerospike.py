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

    def test_create_dict_from_record_with_bins(self):
        mock = ( (self.namespace, self.set, self.key), self.metadata, self.bins)
        mock_parsed = self.operator.create_dict_from_record(record=mock)
        
        expected = {"namespace": self.namespace, "set": self.set, "key": self.key, "metadata": self.metadata, "bins": self.bins}
        assert mock_parsed == expected

    def test_create_dict_from_record_no_bins(self):
        mock = ( (self.namespace, self.set, self.key), self.metadata)
        mock_parsed = self.operator.create_dict_from_record(record=mock)
        
        expected = {"namespace": self.namespace, "set": self.set, "key": self.key, "metadata": self.metadata}
        assert mock_parsed == expected


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

        

