"""This module allows to connect to a Aerospike database."""

import aerospike
from aerospike import Client
from typing import Tuple, overload, List, Union
from airflow.hooks.base import BaseHook
from collections import namedtuple

class AerospikeClientContextManager:
    def __init__(self, client: Client) -> None:
        self.client = client

    def __enter__(self) -> Client:
        return self.client

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.client is not None:
            self.client.close()
            
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
        self.aerospike_conn_id = conn_id
        self.connection = kwargs.pop("connection", None)
        self.client = None

    def get_conn(self) -> Client:
        """
        A method that initiates a new Aerospike connection.
        """
        if self.client is not None:
            return self.client

        self.connection = self.get_connection(self.aerospike_conn_id)
        
        config = {'hosts': [ (self.connection.host, self.connection.port) ]}
        self.log.info('Hosts: %s', config['hosts'][0])

        self.client = aerospike.client(config).connect()
        return self.client

    @overload
    def exists(self, namespace: str, set: str, key: List[str], policy: dict) -> list: ...

    @overload
    def exists(self, namespace: str, set: str, key: str, policy: dict) -> tuple: ...

    def exists(self, key: Union[List[str], str], namespace:str, set: str, policy: dict) -> Union[list, tuple]:
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

    def get_record(self, key: Union[List[str], str], namespace:str, set: str, policy: dict) -> Union[list, tuple]:
        with AerospikeClientContextManager(client=self.get_conn()) as client:
            if isinstance(key, list):
                keys = [(namespace, set, k) for k in key]
                return client.get_many(keys, policy)
            return client.get((namespace, set, key), policy)
        
    #TODO: add delete/delete_many method

    #TODO: add touch method
    
    # TODO: fix this
    def test_connection(self) -> Tuple[bool, str]:
        """Test the Aerospike connection by conneting to it."""
        try:
            with AerospikeClientContextManager(client=self.get_conn()) as client:
                client.is_connected()
        except Exception as e:
            return False, str(e)
        return True, "Connection successfully tested"