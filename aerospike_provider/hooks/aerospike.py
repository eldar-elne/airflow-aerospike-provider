"""This module allows to connect to a Aerospike database."""

import aerospike
from aerospike import Client
from typing import Tuple, overload, List, Union
from airflow.hooks.base import BaseHook
from collections import namedtuple


class AerospikeHook(BaseHook):
    """
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
    def exists(self, namespace: str, set: str, key: List[str], policy: dict): ...

    @overload
    def exists(self, namespace: str, set: str, key: str, policy: dict): ...

    def exists(self, key: Union[List[str], str], namespace:str, set: str, policy: dict={'key': aerospike.POLICY_KEY_SEND}):
        client = self.get_conn()
        if isinstance(key, list):
            keys = [(namespace, set, k) for k in key]
            return client.exists_many(keys, policy)
        return client.exists((namespace, set, key), policy)


    #TODO: add put/put_many method

    #TODO: add delete/delete_many method

    #TODO: add touch method
    
    #TODO: add get/get many method
    

    # TODO: fix this
    def test_connection(self) -> Tuple[bool, str]:
        """Test the Aerospike connection by conneting to it."""
        # try:
        #     self.get_conn().is_connected()
        # except Exception as e:
        #     return False, str(e)
        # return True, "Connection successfully tested"
        ...