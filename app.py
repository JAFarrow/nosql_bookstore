import boto3

def create_table(client):
    try:
        resp = client.create_table(
            TableName="Books",
            KeySchema=[
                {
                    "AttributeName": "Author",
                    "KeyType": "HASH"
                },
                {
                    "AttributeName": "Title",
                    "KeyType": "RANGE"
                },
            ],
            AttributeDefinitions=[
                {
                    "AttributeName": "Author",
                    "AttributeType": "S"
                },
                {
                    "AttributeName": "Title",
                    "AttributeType": "S"
                }
            ],
            ProvisionedThroughput={
                "ReadCapacityUnits": 1,
                "WriteCapacityUnits": 1
            }
        )
        print("Table Created Successfully!")
    except Exception as e:
        print("Error Creating Table..")
        print(e)

def update_table(client):
    try:
        resp = client.update_table(
            TableName="Books",
            AttributeDefinitions=[
                {
                    "AttributeName": "Category",
                    "AttributeType": "S"
                }
            ],
            GlobalSecondaryIndexUpdates=[
                {
                    "Create": {
                        "IndexName": "CategoryIndex",
                        "KeySchema": [
                            {
                                "AttributeName": "Category",
                                "KeyType": "HASH"
                            }
                        ],
                        "Projection": {
                            "ProjectionType": "ALL"
                        },
                        "ProvisionedThroughput": {
                            "ReadCapacityUnits": 1,
                            "WriteCapacityUnits": 1
                        }
                    }
                }
            ]
        )
        print("Secondary Index Added!")
    except Exception as e:
        print("Error Updating Table...")
        print(e)

def update(client):
    update_resp = client.execute_statement(Statement='UPDATE Books\
        SET Formats.Audiobook = \'JCV555\'\
        WHERE\
        Author = \'Antje Barth\' AND Title = \'Data Science on AWS\'')
    
def delete(client): 
    delete_resp = client.execute_statement(Statement='UPDATE Books\
    REMOVE Formats.Audiobook\
    WHERE\
    Author = \'Antje Barth\' AND Title = \'Data Science on AWS\'')

def delete_table(client):
    try:
        resp = client.delete_table(
            TableName="Books"
        )
        print("Table deleted successfully!")
    except Exception as e:
        print("Error deleting table:")
        print(e)

def main():
    select_query = 'SELECT * FROM Books WHERE Author = \'Antje Barth\' AND Title = \'Data Science on AWS\''

    client = boto3.client('dynamodb', region_name='eu-west-1')

    # create_table(client)
    select_resp = client.execute_statement(Statement=select_query)
    print("Singular select on one primary key:\n\n", select_resp['Items'])

    # update_table(client)
    secondary_select_resp = client.execute_statement(Statement='SELECT * FROM Books.CategoryIndex WHERE Category = \'Technology\'')
    print("\nSelect all on newly added index:\n\n", secondary_select_resp['Items'])

    update(client)
    select_resp = client.execute_statement(Statement=select_query)
    print('\nInitial select after update:\n\n', select_resp['Items'])

    delete(client)
    select_resp = client.execute_statement(Statement=select_query)
    print('\nInitial select after deletion/reset\n\n', select_resp['Items'])

    # delete_table(client)

if __name__ == "__main__":
    main()