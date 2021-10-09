

import athenaHandler
import asyncio



def athenaExecute(file, fileName):
    with open(file, 'r') as queryFile:
        query = queryFile.read().replace('\n', ' ')

    qa = athenaHandler.QueryAthena(query=query, database='marwebapp_db', athenaResultBucket = 'marwebapp-test', 
    athenaResultFolder = 'queryFinalResult/', finalResultBucket = 'marwebappanalytics', resultFileName = fileName + '.csv')
    df = qa.run_query()
    qa.writeData(df)
    return df.iloc[1,:].to_string


async def athenaQueue(queryFileArr, queryFileNameArr):
    loop = asyncio.get_running_loop()
    objects = await asyncio.gather(
        *[
            loop.run_in_executor(None, athenaExecute(query, queryFileNameArr[i]))
            for i, query in enumerate(queryFileArr)
        ]
    )
    return objects

def lambda_handler(event, context):
    print('received event:')
    print(event)
    
    sqlFileNames = ['whoQuery.txt', 'sportQuery.sql']
    outputFileNames = ['whoquery', 'sportquery']
    
    
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(athenaQueue(sqlFileNames, outputFileNames))
    
    

    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': print(result)
    }
