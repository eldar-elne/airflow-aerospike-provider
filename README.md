<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# Aerospike Provider for Apache Airflow
![image](https://github.com/eldar-eln-bigabid/airflow-aerospike-provider/assets/116807288/6e838f7c-f534-4384-8a61-08de5e60cf5a)


<a id="installation"></a>
## Installation
requirements:
`python` 3.8.0+
`aerospike` 14.0.0+
`apache-airflow` 2.2.0+

You can install this package as:
```shell
pip install airflow-provider-aerospike
```

## Configuration

In the Airflow interface, configure a `Connection` for Aerospike. Configure the following fields:
* `Conn Id`: `aerospike_conn_id`
* `Conn Type`: `Aerospike`
* `Port`: Aerospike cluster port (usually at 3000)
* `Host`: Cluster node address (The client will learn about the other nodes in the cluster from the seed node)


### Operators
currently, the provider supports simple operations such as Fetching single or multiple keys and Creating/Updating keys.

### Sensors
currently, the provider supports simple methods such as checking if single or multiple keys exist.
