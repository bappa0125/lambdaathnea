import time
import boto3
import json

# athena constant
DATABASE = 'default'
TABLE = 'cloudtrail_logs'

# S3 constant
S3_OUTPUT = 's3://subrata-aws-youtube'
S3_BUCKET = 'subrata-aws-youtube'

# number of retries
RETRY_COUNT = 50
EVENT_NAME = ''
USER_NAME = ''
# query constant
COLUMN = 'useridentity.type'


def lambda_handler(event, context):
    print(event)
    USER_NAME = event['params']['querystring']['user']
    EVENT_NAME =event['params']['querystring'] ['eventname']
    # get keyword
  
    #keyword = event['user']
    

    # created query
    query = "SELECT count(*) as subnet_count FROM %s.%s where useridentity.type = '%s' AND eventname='%s'" % (DATABASE, TABLE, USER_NAME, EVENT_NAME)
    print(query)

    # athena client
    client = boto3.client('athena')

    # Execution
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': DATABASE
        },
        ResultConfiguration={
            'OutputLocation': S3_OUTPUT,
        }
    )

    # get query execution id
    query_execution_id = response['QueryExecutionId']
    print(query_execution_id)
    print('hi')

    # get execution status
    for i in range(1, 1 + RETRY_COUNT):

        # get query execution
        query_status = client.get_query_execution(QueryExecutionId=query_execution_id)
        print("query status")
        print(query_status)
        query_execution_status = query_status['QueryExecution']['Status']['State']
        print(query_execution_status)
        print(i)
        if query_execution_status == 'SUCCEEDED':
            print("STATUS success:" + query_execution_status)
            break

        if query_execution_status == 'FAILED':
            raise Exception("STATUS failed:" + query_execution_status)

        else:
            print("STATUS:" + query_execution_status)
            time.sleep(i)
    else:
        client.stop_query_execution(QueryExecutionId=query_execution_id)
        raise Exception('TIME OVER')

    # get query results
    result = client.get_query_results(QueryExecutionId=query_execution_id)
    print(result)

    # get data
    if len(result['ResultSet']['Rows']) == 2:
        print(len(result['ResultSet']['Rows']))
        print(result['ResultSet']['Rows'][1])
        resource_count = int(result['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])
        if int(resource_count) == 4 and EVENT_NAME == 'CreateSubnet':
            
            #return "hi"
            return {
            "statusCode": 200,
            "body": {
                "message": "All the steps were perfromed properly",
                "user" : USER_NAME,
                "status" : "Pass",
                "event" : EVENT_NAME,
                "total_resource_affected" : resource_count
                # "location": ip.text.replace("\n", "")
            }
            }
        elif (resource_count >= 1) :

            return {
            "statusCode": 200,
            "body": {
                "message": "All the steps were perfromed properly",
                "user" : USER_NAME,
                "status" : "Pass",
                "event" : EVENT_NAME,
                "total_resource_affected" : resource_count
                # "location": ip.text.replace("\n", "")
            }
            }

        else:
            return {
            "statusCode": 200,
            "body": {
                "message": "All the steps were not performed  correctly",
                "user" : USER_NAME,
                "status" : "Fail",
                "event" : EVENT_NAME, 
                "total_resource_affected" : resource_count
                # "location": ip.text.replace("\n", "")
            },

    }

    else:
        return  {
        "statusCode": 200,
        
            "message": "problem in running th query",
            # "location": ip.text.replace("\n", "")
        }
    