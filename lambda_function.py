import athenaQueueHandler
import asyncio



def lambda_handler(event, context):
    print('received event:')
    print(event)
    
    sqlFileNames = ['whoQuery.txt', 'sportQuery.sql']
    outputFileNames = ['whoquery', 'sportquery']
    queriesFolder = 'queries'
    
    
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(athenaQueueHandler.athenaQueue(queriesFolder, sqlFileNames, outputFileNames))
    
    

    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': print(result)
    }
