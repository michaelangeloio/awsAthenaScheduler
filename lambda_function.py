


import athenaQueueHandler
import asyncio



def lambda_handler(event, context):
    print('received event:')
    print(event)
    
    queryQueue = [
        {
            'queryFile' : 'whoQuery.sql',
            'outputName': 'whoquery' 
            
        },
        {
            'queryFile': 'sportQuery.sql',
            'outputName': 'sportquery' 
            
        },
        {
            'queryFile': 'foodQuery.sql',
            'outputName': 'foodquery' 
            
        },
        {
            'queryFile': 'countQuery.sql',
            'outputName': 'countquery' 
            
        },
        {
            'queryFile': 'locationQuery.sql',
            'outputName': 'locationquery' 
            
        },
        {
            'queryFile': 'playedgameQuery.sql',
            'outputName': 'playedgame' 
            
        },
        
    ]
    
    queriesFolder = 'queries'
    
    
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(athenaQueueHandler.athenaQueue(queriesFolder, queryQueue))
    
    

    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': print(result)
    }
