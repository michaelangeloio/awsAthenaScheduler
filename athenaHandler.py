import time
import boto3
import pandas as pd
import io
import json
from botocore.exceptions import ClientError
import os
from io import StringIO


class QueryAthena:

    def __init__(self, query, database, athenaResultBucket, athenaResultFolder, finalResultBucket, resultFileName):
        self.database = database
        self.folder = athenaResultFolder
        self.bucket = athenaResultBucket
        self.finalResultBucket = finalResultBucket
        self.resultFileName = resultFileName
        self.s3_input = 's3://' + self.bucket + '/my_folder_input'
        self.s3_output =  's3://' + self.bucket + '/' + self.folder
        self.region_name = 'us-east-1'
        self.aws_access_key_id = "my_aws_access_key_id"
        self.aws_secret_access_key = "my_aws_secret_access_key"
        self.query = query

    def load_conf(self, q):
        try:
            self.client = boto3.client('athena')
                            #   region_name = self.region_name, 
                            #   aws_access_key_id = self.aws_access_key_id,
                            #   aws_secret_access_key= self.aws_secret_access_key)
            response = self.client.start_query_execution(
                QueryString = q,
                    QueryExecutionContext={
                    'Database': self.database
                    },
                    ResultConfiguration={
                    'OutputLocation': self.s3_output,
                    }
            )
            self.filename = response['QueryExecutionId']
            print('Execution ID: ' + response['QueryExecutionId'])

        except Exception as e:
            print(e)
        return response                

    def run_query(self):
        queries = [self.query]
        for q in queries:
            res = self.load_conf(q)
        try:              
            query_status = None
            while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
                query_status = self.client.get_query_execution(QueryExecutionId=res["QueryExecutionId"])['QueryExecution']['Status']['State']
                print(query_status)
                if query_status == 'FAILED' or query_status == 'CANCELLED':
                    raise Exception('Athena query with the string "{}" failed or was cancelled'.format(self.query))
                time.sleep(10)
            print('Query "{}" finished.'.format(self.query))

            df = self.obtain_data()
            return df

        except Exception as e:
            print(e)      

    def obtain_data(self):
        try:
            self.resource = boto3.resource('s3')
                                #   ,region_name = self.region_name, 
                                #   aws_access_key_id = self.aws_access_key_id,
                                #   aws_secret_access_key= self.aws_secret_access_key)

            response = self.resource \
            .Bucket(self.bucket) \
            .Object(key= self.folder + self.filename + '.csv') \
            .get()

            return pd.read_csv(io.BytesIO(response['Body'].read()), encoding='utf8')   
        except Exception as e:
            print(e)  
            




    def writeData(self, df):
        try:
            self.putresource = boto3.resource('s3')
            print("writing data to " + self.finalResultBucket)
            csv_buffer = StringIO()
            df.to_csv(csv_buffer)
            self.putresource.Object(self.finalResultBucket, self.resultFileName).put(Body=csv_buffer.getvalue())
        except Exception as e:
            print(e)
            
            
            


def lambda_handler(event, context):
    # print('received event:')
    # print(event)

    query = "SELECT * FROM form_who;"
    qa = QueryAthena(query=query, database='marwebapp_db', athenaResultBucket = 'marwebapp-test', 
    athenaResultFolder = 'queryFinalResult/', finalResultBucket = 'marwebappanalytics', resultFileName = 'test.csv')
    df = qa.run_query()
    qa.writeData(df)
    # test = wr.athena.read_sql_query("SELECT * FROM form_who", database="marwebapp_db", ctas_approach=False)    
    # print(df.head())
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': print(df.head())
    }
