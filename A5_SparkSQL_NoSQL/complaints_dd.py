# create and then store the results into a DynamoDB table,
# and then use DynamoDB to calculate the results



# insert into DynamoDB
if __name__ == "__main__":

    import json
    import urllib
    import requests
    import datetime

    data = json.loads(
        urllib.request.urlopen("http://data.consumerfinance.gov/api/views/7zpz-7ury/rows.json").read().decode('utf-8'))

    ## create DynamoDB table
    import boto3
    from boto3.dynamodb.conditions import Key, Attr

    # Get the service resource.
    dynamodb = boto3.resource('dynamodb',region_name = "us-east-1",endpoint_url="http://localhost:8000")

    # Create the DynamoDB table.

    # retrieve table
    # table = dynamodb.Table('rw848')
    table = dynamodb.create_table(
        TableName='rw848',
        KeySchema=[
            {
                'AttributeName': 'Year',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'sid',
                'KeyType': 'RANGE'
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'Year',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'sid',
                'AttributeType': 'N'
            }

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 123,
            'WriteCapacityUnits': 123
        }
    )

    # Wait until the table exists.

    table.meta.client.get_waiter('table_exists').wait(TableName='rw848')

    # Print out some data about the table.
    # print(table.item_count)
    # print(table.creation_date_time)

    # calculate
    year_lis = []

    for ele in data['data']:
        if ele[8][0:4] not in year_lis:
            year_lis.append(ele[8][0:4])
        table.put_item(
            Item = {
                'Year': ele[8][0:4],
                'sid': ele[0]
            }
        )

    year_lis.sort()

    # query from DynamoDB
    with open("complaints_dd.txt","w") as f:
        for i in year_lis:
            count = table.query(Select = 'COUNT',KeyConditionExpression = Key('Year').eq(i))
            print("%s %d"%(i,count['Count']))
            f.write("%s %d\n"%(i,count['Count']))
