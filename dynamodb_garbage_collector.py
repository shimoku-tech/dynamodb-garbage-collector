import boto3
import botocore
import datetime
import time
from typing import List, Dict, Any, Type
from concurrent.futures import ThreadPoolExecutor, Future
from threading import Lock
from logging import Logger

def _delete_items(client: Type[boto3.client], table: str, 
                  items: List[Dict[str, Any]]) -> int:
    # Create a list of delete requests for each item
    unprocessed_items = [{'DeleteRequest':{'Key': item}} for item in items]

    # Backoff time for retrying requests that fail due to exceeded throughput
    backoff = 1

    while unprocessed_items: 
        response = None
        try:
            # Perform the batch write request
            response = client.batch_write_item(
                RequestItems={
                    table: unprocessed_items
                })
        except client.exceptions.ProvisionedThroughputExceededException:
            # If the request fails due to exceeded throughput, sleep and
            # retry with an increased backoff time            
            time.sleep(backoff)
            backoff *= 2
        else:
            # Reset the backoff time if the request succeeded
            backoff = 1

        # Check if there are any unprocessed items and retry them in the 
        # next iteration if there are
        if response:
            unprocessed_items = response.get('UnprocessedItems', {}).get(table, [])

    return len(items)

def purge_orphan_items(logger: Logger, region: str, parent_table: str, child_table: str, 
                       key_attribute: str, child_reference_attribute: str, 
                       max_workers: int = 100, timestamp_attribute: str = None, 
                       timestamp_format: str = None) -> None:
    logger.info(f'Purge of orphaned items in {child_table} referencing a '
                f'non-existent item in {parent_table} started')
    
    # Initialize DynamoDB client with specified AWS region and 
    # max_pool_connections that is slightly larger than max_workers
    client = boto3.client(
        "dynamodb", 
        region_name=region, 
        config=botocore.config.Config(max_pool_connections=max_workers+10))

    # Initialize variables used to track deleted items and execute deletion operations 
    # concurrently
    deleted_items_counter = 0
    last_logged_counter = 0
    lock = Lock()  
    executor = ThreadPoolExecutor(max_workers=max_workers)

    # Closure of purge_orphan_items used to submit a delete items operation
    def submit_delete_items(table: str, items: List):        
        # Callback called when a delete items operation completes 
        # Lock used to ensure variables deleted_items_counter and last_logged_counter 
        # of purge_orphan_items are updated atomically        
        def delete_items_callback(future: Future):
            nonlocal deleted_items_counter
            nonlocal last_logged_counter
            with lock:
                deleted_items_counter += future.result()
                if deleted_items_counter - last_logged_counter >= 100:
                    logger.info(f'Total deleted items in {table}: {deleted_items_counter}')
                    last_logged_counter = deleted_items_counter
                    
        # Wait until the size of the work queue of the executor is less than 10
        while executor._work_queue.qsize() >= 10:
            pass
        future = executor.submit(
            _delete_items, 
            client=client,
            table=table,
            items=items)
        future.add_done_callback(delete_items_callback)    

    # Query the parent table to get all parent ids
    parent_table_ids = []
    query_response = client.scan(
        TableName=parent_table, 
        ProjectionExpression=f'#{key_attribute}',
        ExpressionAttributeNames={
            f'#{key_attribute}': key_attribute
        })

    scanned_parent_items = 0

    # Process all pages returned by the scan
    while 'LastEvaluatedKey' in query_response:
        for item in query_response['Items']:
            parent_table_ids.append(item[key_attribute]['S'])
            scanned_parent_items += 1
            if scanned_parent_items % 1000 == 0:
                logger.info(f'Total scanned items in {parent_table}: {scanned_parent_items}')
        query_response = client.scan(TableName=parent_table, 
                                     ProjectionExpression=f'#{key_attribute}',
                                     ExpressionAttributeNames={
                                         f'#{key_attribute}': key_attribute
                                     },
                                     ExclusiveStartKey=query_response['LastEvaluatedKey'])

    # Process the final page of results
    for item in query_response['Items']:
        parent_table_ids.append(item[key_attribute]['S'])
        scanned_parent_items += 1        

    logger.info(f'Total scanned items in {parent_table}: {scanned_parent_items}')

    # Set the maximum timestamp for the records to delete (e.g., one hour ago)
    # This will be used only if the timestamp condition should be applied
    max_timestamp = datetime.datetime.utcnow() - datetime.timedelta(hours=1)                

    # Iterate through the child table and delete records whose child_reference_attribute
    # is not in the parent_table_ids list and, if optional timestamp attributes are provided, 
    # whose timestamp attribute is earlier than the specified maximum time    
    expression_attribute_names = {
        f'#{key_attribute}': key_attribute,
        f'#{child_reference_attribute}': child_reference_attribute 
    }
    projection_expression = f'#{key_attribute}, #{child_reference_attribute}'
    if timestamp_attribute is not None and timestamp_format is not None:
        expression_attribute_names[f'#{timestamp_attribute}'] = timestamp_attribute
        projection_expression = f'#{key_attribute}, #{child_reference_attribute}, #{timestamp_attribute}'

    response = client.scan(
        TableName=child_table, 
        ProjectionExpression=projection_expression,
        ExpressionAttributeNames=expression_attribute_names
    )
    scanned_child_items = 0
    unprocessed_orphan_items = []
    while 'LastEvaluatedKey' in response:
        for item in response['Items']:
            scanned_child_items += 1
            if scanned_child_items % 1000 == 0:
                logger.info(f'Total scanned items in {child_table}: {scanned_child_items}')

            if timestamp_attribute is not None and timestamp_format is not None:
                # Check if the item's timestamp attribute is after the maximum timestamp 
                # If it is, skip the item and continue to the next one
                if item[timestamp_attribute]['S'] > max_timestamp.strftime(timestamp_format):
                    continue

            if item[child_reference_attribute]['S'] not in parent_table_ids:
                unprocessed_orphan_items.append({key_attribute: item[key_attribute]})
                # Begins delete operation when accumulated items in unprocessed_orphan_items reaches 25
                if len(unprocessed_orphan_items) == 25:
                    submit_delete_items(child_table, unprocessed_orphan_items)
                    unprocessed_orphan_items = []            

        response = client.scan(
            TableName=child_table,
            ProjectionExpression=projection_expression,
            ExpressionAttributeNames=expression_attribute_names,
            ExclusiveStartKey=response['LastEvaluatedKey'])      

    # Delete remaining items in the final page of the scan
    for item in response['Items']:
        scanned_child_items += 1

        if timestamp_attribute is not None and timestamp_format is not None:
            # Check if the item's timestamp attribute is after the maximum timestamp 
            # If it is, skip the item and continue to the next one
            if item[timestamp_attribute]['S'] > max_timestamp.strftime(timestamp_format):
                continue

        if item[child_reference_attribute]['S'] not in parent_table_ids:
            unprocessed_orphan_items.append({key_attribute: item[key_attribute]})
            # Begins delete operation when accumulated items in unprocessed_orphan_items reaches 25
            if len(unprocessed_orphan_items) == 25:
                submit_delete_items(child_table, unprocessed_orphan_items)
                unprocessed_orphan_items = []   

    # If there are remaining items in unprocessed_orphan_items proceed with delete operation
    if len(unprocessed_orphan_items) > 0:
        submit_delete_items(child_table, unprocessed_orphan_items)
        unprocessed_orphan_items = []

    logger.info(f'Total scanned items in {child_table}: {scanned_child_items}')         

    # Shutdown the executor and wait for all tasks to complete
    executor.shutdown(wait=True)

    logger.info(f'Purge of orphaned items in {child_table} referencing a '
                f'non-existent item in {parent_table} finished with total '
                f'deleted items: {deleted_items_counter}')