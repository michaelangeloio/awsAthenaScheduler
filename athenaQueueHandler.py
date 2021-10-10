
import athenaHandler
import asyncio


def athenaExecute(queryfolder, file, fileName):
    with open(queryfolder + "/" + file, 'r') as queryFile:
        query = queryFile.read().replace('\n', ' ')

    qa = athenaHandler.QueryAthena(query=query, database='marwebapp_db', athenaResultBucket = 'marwebapp-test', 
    athenaResultFolder = 'queryFinalResult/', finalResultBucket = 'marwebappanalytics', resultFileName = fileName + '.csv')
    df = qa.run_query()
    qa.writeData(df)
    return df.head(2).to_string


async def athenaQueue(queryfolder, queue):
    loop = asyncio.get_running_loop()
    objects = await asyncio.gather(
        *[
            loop.run_in_executor(None, athenaExecute(queryfolder, query['queryFile'], query['outputName']))
            for query in queue
        ]
    )
    return objects
