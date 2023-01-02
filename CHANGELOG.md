# CHANGELOG

## 1.1.0 (2023-01-02)

### Improved performance of `purge_orphan_items` function by implementing batch item deletions

* Replaced usage of Boto3 DynamoDB client `delete_item` with `batch_write_item` for deleting items in DynamoDB. This change improves performance by reducing the number of requests made to DynamoDB and allowing for batch deletions.
* Added internal `_delete_items` function to handle the `batch_write_item` operation.  
* Added closure `submit_delete_items` to the `purge_orphan_items` function, which contains the `executor.submit` call to `_delete_items` and the `delete_items_callback` function.

## 1.0.0 (2023-01-01)

### Initial release with `purge_orphan_items` function

* The `purge_orphan_items` function has been added to delete orphan garbage items in DynamoDB tables. This function allows you to specify two tables (`parent_table` and `child_table`), a key attribute for both tables (`key_attribute`), a reference attribute in the child table (`child_reference_attribute`), and an optional timestamp attribute in the child table (`timestamp_attribute`). The function performs a scan of the parent table and collects all the IDs of the items in that table. Then, it scans the child table and deletes those items whose reference attribute is not found in the list of parent table IDs and, if optional timestamp attribute is provided, whose timestamp attribute is earlier than a specified maximum time (by default, one hour ago). A `ThreadPoolExecutor` is used to perform the deletion operations concurrently and a counter of deleted items is displayed during execution.