"""This module allows to connect to a Aerospike database."""

import aerospike
from aerospike import Client
from typing import Tuple, overload, List, Union, Dict
from types import TracebackType
from airflow.hooks.base import BaseHook

class AerospikeClientContextManager:
    def __init__(self, client: Client) -> None:
        self.client = client

    def __enter__(self) -> Client:
        return self.client

    def __exit__(
        self, 
        exc_type: Union[BaseException, None],
        exc_val: Union[BaseException, None], 
        exc_tb: Union[TracebackType, None]
        ) -> None:
        if self.client is not None:
            self.client.close()
            self.client = None
            
class AerospikeHook(BaseHook):
    """
    Interact with Aerospike.
    
    Creates a client to Aerospike and runs a command.

    :param aerospike_conn_id: Reference to :ref:`Aerospike connection id`.
    """

    conn_name_attr = 'aerospike_conn_id'
    default_conn_name = 'aerospike_default'
    conn_type = 'aerospike'
    hook_name = 'Aerospike'

    def __init__(self, conn_id: str = default_conn_name, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.connection = kwargs.pop("connection", None)
        self.client = None

    def get_conn(self) -> Client:
        """
        A method that initiates a new Aerospike connection.
        """
        if self.client is not None:
            return self.client

        self.connection = self.get_connection(self.conn_id)
        
        config = {'hosts': [ (self.connection.host, self.connection.port) ]}
        self.log.info('Hosts: %s', config['hosts'][0])

        self.client = aerospike.client(config).connect()
        return self.client

    @overload
    def exists(self, namespace: str, set: str, key: List[str], policy: dict) -> list: ...

    @overload
    def exists(self, namespace: str, set: str, key: str, policy: dict) -> tuple: ...

    def exists(self, namespace:str, set: str, key: Union[List[str], str], policy: dict) -> Union[list, tuple]:
        with AerospikeClientContextManager(client=self.get_conn()) as client:
            if isinstance(key, list):
                keys = [(namespace, set, k) for k in key]
                return client.exists_many(keys, policy)
            return client.exists((namespace, set, key), policy)


    def put(self, key: str, bins: dict, metadata: dict, namespace: str, set: str, policy: dict) -> None:
        with AerospikeClientContextManager(client=self.get_conn()) as client:
            return client.put((namespace, set, key), bins, metadata, policy)

    @overload
    def get_record(self, namespace: str, set: str, key: List[str], policy: dict) -> list: ...

    @overload
    def get_record(self, namespace: str, set: str, key: str, policy: dict) -> tuple: ...

    def get_record(self, namespace:str, set: str, key: Union[List[str], str], policy: dict) -> Union[list, tuple]:
        with AerospikeClientContextManager(client=self.get_conn()) as client:
            if isinstance(key, list):
                keys = [(namespace, set, k) for k in key]
                return client.get_many(keys, policy)
            return client.get((namespace, set, key), policy)
        
    def touch_record(self, namespace: str, set: str, key: str, ttl: int, policy: dict = None) -> None:
        with AerospikeClientContextManager(client=self.get_conn()) as client:
            client.touch(key=(namespace, set, key), val=ttl, policy=policy)
    
    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ["schema", "login", "password"],
            "relabeling": {
                "host": "host",
                "port": "port"
            },
            "placeholders": {
                "port": "3000",
                "host": "cluster node address (The client will learn about the other nodes in the cluster from the seed node)"
            },
        }
        
    # TODO: fix this
    def test_connection(self) -> Tuple[bool, str]:
        """Test the Aerospike connection by conneting to it."""
        try:
            with AerospikeClientContextManager(client=self.get_conn()) as client:
                client.is_connected()
        except Exception as e:
            return False, str(e)
        return True, "Connection successfully tested"