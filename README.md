# DynamoDB Garbage Collector

[![Version](https://img.shields.io/badge/version-0.0.0-blue.svg)](https://shields.io/)

The DynamoDB Garbage Collector is a Python library that allows you to delete garbage items in DynamoDB tables.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Installation

To install the DynamoDB Garbage Collector, use `pip`:

```bash
$ pip install dynamodb-garbage-collector
```

## Usage

The DynamoDB Garbage Collector currently provides a single function called `purge_orphan_items`, which allows you to delete orphan items in a child table that reference a non-existent item in a parent table. If optional timestamp attributes are provided only will be delete orphan items earlier than a specified maximum time (by default, one hour ago).

To use `purge_orphan_items`, you need to provide the following parameters:

- `logger`: a logger object to log messages during the execution of the function.
- `region`: the AWS region where the parent and child tables are located.
- `parent_table`: the name of the parent table.
- `child_table`: the name of the child table.
- `key_attribute`: the name of the key attribute for both tables.
- `child_reference_attribute`: the name of the reference attribute in the child table.
- `max_workers` (optional): the maximum number of workers to use for concurrent operations. If not provided, a default value of 100 will be used.
- `timestamp_attribute` (optional): the name of the attribute that contains the timestamp of the records in the child table. If not provided, timestamp will not be taken into account when deleting items.
- `timestamp_format` (optional): the format of the timestamp attribute. If not provided, timestamp will not be taken into account when deleting items.

Here is an example of how to use the `purge_orphan_items` function:

```python
import logging
from dynamodb_garbage_collector import purge_orphan_items

# Set up the logger
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Set the AWS region where the parent and child tables are located
region = 'eu-west-1'

# Set the names of the parent and child tables, and the key and reference attributes
parent_table = 'ParentTable'
child_table = 'ChildTable'
key_attribute = 'id'
child_reference_attribute = 'parentId'

# Set the maximum number of workers
max_workers = 50

# Set the name of the timestamp attribute and the timestamp format
timestamp_attribute = 'createdAt'
timestamp_format = '%Y-%m-%dT%H:%M:%S.%fZ'

# Call the function
purge_orphan_items(logger, region, parent_table, child_table, key_attribute, child_reference_attribute, max_workers, timestamp_attribute, timestamp_format)
```

## Contributing

We welcome contributions to the DynamoDB Garbage Collector. To contribute, please fork the repository and create a pull request with your changes.

## License

The DynamoDB Garbage Collector is released under the MIT License.