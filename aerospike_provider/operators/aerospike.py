from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence, Union, List

if TYPE_CHECKING:
    from airflow.utils.context import Context

import aerospike
from aerospike_provider.hooks.aerospike import AerospikeHook
from airflow.models.baseoperator import BaseOperator


class AerospikePutKey(BaseOperator):
    """
    Create a new record, add or remove bins.
    This can also remove a record (if exists) using `<bin_name>.isnull()` if it's the last bin.

    :param aerospike_conn_id: aerospike connection to use, defaults to 'aerospike_default'
    :param key: key to save in the db.
    :param namespace: namespace to use in aerospike db
    :param set: set name in the namespace
    :param bins: bin name(s) with data saved along with a key. For example: `{"bin": value}`
    :param metadata: metadata about the key eg. ttl. For example: `{"ttl": 0}`
    :param policy: which policy the key should be saved with. default `POLICY_KEY_SEND`
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
        metadata: dict = None,
        policy: dict = {'key': aerospike.POLICY_KEY_SEND},
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
        
        hook = AerospikeHook(self.aerospike_conn_id)
        self.log.info('Storing %s as key', self.key)
        hook.put(key=self.key, bins=self.bins, metadata=self.metadata, namespace=self.namespace, set=self.set, policy=self.policy)
        self.log.info('Stored key successfully')