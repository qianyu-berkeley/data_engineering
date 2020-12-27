"""A utility class to interface with DynamoDB."""
import boto3
from boto3.dynamodb.conditions import Key


class DynamodbAPI:
    def __init__(self, region_name):
        self.region_name = region_name
        self.dynamodb_resource = boto3.resource(
            "dynamodb", region_name=self.region_name
        )
        self.dynamodb_client = boto3.client("dynamodb", region_name=self.region_name)

    def get_table_list(self):
        """Return a list all tables in the dynamoDB."""
        table_list = self.dynamodb_client.list_tables()["TableNames"]
        return table_list

    def get_table_metadata(self, table_name):
        """Get some metadata about chosen table."""
        table = self.dynamodb_resource.Table(table_name)

        return {
            "num_items": table.item_count,
            "primary_key_name": table.key_schema[0],
            "status": table.table_status,
            "bytes_size": table.table_size_bytes,
            "global_secondary_indices": table.global_secondary_indexes,
        }

    def read_table_item(self, table_name, pk_name, pk_value):
        """Return item read by primary key."""
        table = self.dynamodb_resource.Table(table_name)
        response = table.get_item(Key={pk_name: pk_value})

        return response

    def add_item(self, table_name, col_dict):
        """Add one item (row) to table.
        Note: col_dict is a dictionary {col_name: value}.
        """
        table = self.dynamodb_resource.Table(table_name)
        response = table.put_item(Item=col_dict)

        return response

    def delete_item(self, table_name, pk_name, pk_value):
        """Delete an item (row) in table from its primary key."""
        table = self.dynamodb_resource.Table(table_name)
        response = table.delete_item(Key={pk_name: pk_value})

        return response

    def scan_table(self, table_name, filter_key=None, filter_value=None):
        """Perform a scan operation on table.

        Can specify filter_key (col name) and its value to be filtered.
        """
        table = self.dynamodb_resource.Table(table_name)

        if filter_key and filter_value:
            filtering_exp = Key(filter_key).eq(filter_value)
            response = table.scan(FilterExpression=filtering_exp)
        else:
            response = table.scan()

        return response

    def query_table(
        self, table_name, filter_key=None, filter_value=None, index_name=None
    ):
        """Perform a query operation on the table.

        Can specify filter_key (col name) and its value to be filtered.
        Can specify secondary index
        """
        table = self.dynamodb_resource.Table(table_name)

        if filter_key and filter_value and index_name:
            filtering_exp = Key(filter_key).eq(filter_value)
            response = table.query(
                IndexName=index_name, KeyConditionExpression=filtering_exp
            )
        elif filter_key and filter_value:
            filtering_exp = Key(filter_key).eq(filter_value)
            response = table.query(KeyConditionExpression=filtering_exp)
        else:
            print("Error: fiter_key and filter_value need to defined!!")

        return response

    def query_table_allpages(
        self, table_name, filter_key=None, filter_value=None, index_name=None
    ):
        """Perform a query operation on the table.

        Can specify filter_key (col name) and its value to be filtered.
        Can specify secondary index
        This gets all pages of results. Returns list of items.
        """
        table = self.dynamodb_resource.Table(table_name)

        if filter_key and filter_value and index_name:
            filtering_exp = Key(filter_key).eq(filter_value)
            response = table.query(
                IndexName=index_name, KeyConditionExpression=filtering_exp
            )
        elif filter_key and filter_value:
            filtering_exp = Key(filter_key).eq(filter_value)
            response = table.query(KeyConditionExpression=filtering_exp)
        else:
            print("Error: fiter_key and filter_value need to defined!!")

        items = response["Items"]
        while True:
            print("Num of items of the page: {}".format(len(response["Items"])))
            if response.get("LastEvaluatedKey"):
                print("Start next page on {}".format(response["LastEvaluatedKey"]))
                if filter_key and filter_value and index_name:
                    filtering_exp = Key(filter_key).eq(filter_value)
                    response = table.query(
                        IndexName=index_name,
                        ExclusiveStartKey=response["LastEvaluatedKey"],
                        KeyConditionExpression=filtering_exp,
                    )
                elif filter_key and filter_value:
                    filtering_exp = Key(filter_key).eq(filter_value)
                    response = table.query(
                        KeyConditionExpression=filtering_exp,
                        ExclusiveStartKey=response["LastEvaluatedKey"],
                    )
                else:
                    print("Error: fiter_key and filter_value need to defined!!")
                items.extend(response["Items"])
            else:
                break

        return items

    def scan_table_allpages(self, table_name, filter_key=None, filter_value=None):
        """Perform a scan operation on table.

        Can specify filter_key (col name) and its value to be filtered.
        This gets all pages of results. Returns list of items.
        """
        table = self.dynamodb_resource.Table(table_name)

        if filter_key and filter_value:
            filtering_exp = Key(filter_key).eq(filter_value)
            response = table.scan(FilterExpression=filtering_exp)
        else:
            response = table.scan()

        items = response["Items"]
        while True:
            print("Num of items of the page: {}".format(len(response["Items"])))
            if response.get("LastEvaluatedKey"):
                print("Start next page on {}".format(response["LastEvaluatedKey"]))
                if filter_key and filter_value:
                    filtering_exp = Key(filter_key).eq(filter_value)
                    response = table.scan(
                        ExclusiveStartKey=response["LastEvaluatedKey"],
                        FilterExpression=filtering_exp,
                    )
                else:
                    response = table.scan(
                        ExclusiveStartKey=response["LastEvaluatedKey"]
                    )
                items.extend(response["Items"])
            else:
                break

        return items

    def update_item(
        self, table_name, pk_name, pk_value, update_expression, expression_attr_values
    ):
        """Perform update on a column."""
        table = self.dynamodb_resource.Table(table_name)
        response = table.update_item(
            Key={pk_name: pk_value},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attr_values,
            ReturnValues="UPDATED_NEW",
        )
        return response
