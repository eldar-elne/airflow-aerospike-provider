from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence, Union, List

if TYPE_CHECKING:
    from airflow.utils.context import Context

import aerospike
from aerospike_provider.hooks.aerospike import AerospikeHook
from airflow.sensors.base import BaseSensorOperator


class AerospikeKeySensor(BaseSensorOperator):
    """
    Check if a key or a set of keys exists in Aerospike given key(s).
    If the key is not found, it will return False.
    When sending multiple keys, the sensor expectes them all for a successful poke.

    :param aerospike_conn_id: aerospike connection to use, defaults to 'aerospike_default'
    :param key: key to search. can be a single key or a list of keys
    :param namespace: namespace to use in aerospike db
    :param set: set name in the namespace
    :param policy: which policy the key should be saved with. default `POLICY_KEY_SEND`
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

    def parse_records(self, records: Union[List, tuple]) -> bool:
        if isinstance(records, list):
            metadata = all(record[1] for record in records)
        elif isinstance(records, tuple):
            metadata = True if records[1] else False
        else:
            raise ValueError(f"Expecting 'list' or 'tuple', got: {type(records)}")
        return metadata

    def poke(self, context: Context) -> bool:

        with AerospikeHook(self.aerospike_conn_id) as hook:
            self.log.info('Poking %s keys', len(self.key))
            records = hook.exists(namespace=self.namespace, set=self.set, key=self.key, policy=self.policy)
            return self.parse_records(records=records)
