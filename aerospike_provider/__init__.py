__version__ = "1.0.0"

from typing import Any, Dict


def get_provider_info() -> Dict[str, Any]:
    return {
        "package-name": "airflow-provider-aerospike",
        "name": "Aerospike Provider",
        "description": "A Aerospike provider for Apache Airflow.",
        "hook-class-names": ["aerospike_provider.hooks.aerospike.AerospikeHook"]
    }