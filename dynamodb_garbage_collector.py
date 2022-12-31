import boto3
import botocore
import datetime
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

def purge_orphan_items(logger, region, parent_table, child_table, key_attribute, 
                       child_reference_attribute, max_workers=100, 
                       timestamp_attribute=None, timestamp_format=None):
    logger.info(f'Purge of orphaned items in {child_table} referencing a '
                f'non-existent item in {parent_table} started')
    
    # Initialize DynamoDB client with specified AWS region and 
    # max_pool_connections that is slightly larger than max_workers
    client = boto3.client(
        "dynamodb", 
        region_name=region, 
        config=botocore.config.Config(max_pool_connections=max_workers+10))

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

    # Function called when a delete item operation completes 
    # Lock used to ensure deleted_items_counter is updated atomically
    deleted_items_counter = 0
    lock = Lock()
    def delete_item_callback(future):
        nonlocal deleted_items_counter
        with lock:
            deleted_items_counter += 1
            if deleted_items_counter % 100 == 0:
                logger.info(f'Total deleted items in {child_table}: {deleted_items_counter}')

    # Initialize a ThreadPoolExecutor with max workers    
    executor = ThreadPoolExecutor(max_workers=max_workers)    

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
                # Wait until the size of the work queue of the executor is less than 10
                while executor._work_queue.qsize() >= 10: 
                    pass

                # Submit the delete_item operation to the executor as a task to be 
                # executed in a separate thread
                future = executor.submit(
                    client.delete_item, 
                    TableName=child_table, 
                    Key={key_attribute: item[key_attribute]})
                future.add_done_callback(delete_item_callback)               

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
            # Wait until the size of the work queue of the executor is less than 10
            while executor._work_queue.qsize() >= 10: 
                pass

            # Submit the delete_item operation to the executor as a task to be 
            # executed in a separate thread
            future = executor.submit(
                client.delete_item, 
                TableName=child_table, 
                Key={key_attribute: item[key_attribute]})
            future.add_done_callback(delete_item_callback)

    logger.info(f'Total scanned items in {child_table}: {scanned_child_items}')         

    # Shutdown the executor and wait for all tasks to complete
    executor.shutdown(wait=True)

    logger.info(f'Purge of orphaned items in {child_table} referencing a '
                f'non-existent item in {parent_table} finished with total '
                f'deleted items: {deleted_items_counter}')
