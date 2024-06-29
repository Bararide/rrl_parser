import asyncio
import aiohttp
import time

from mongo_handler import MongoHandler
from parser_handler import ParserHandler

async def main() -> None:
    async with aiohttp.ClientSession() as session:
        try:
            mongoHandler = MongoHandler()
            parserHandler = ParserHandler(session=session, mongoHandler=mongoHandler)

            await mongoHandler.clear_stations_collection()
            await mongoHandler.clear_trains_collection()
            await mongoHandler.clear_companies_collection()

            start_time = time.time()

            await parserHandler.get_all_stations()

            end_time = time.time()
            execution_time = end_time - start_time
            print("Request execution time:", execution_time, "seconds")

            start_time = time.time()

            await parserHandler.get_all_companies()

            end_time = time.time()
            execution_time = end_time - start_time
            print("Request execution time:", execution_time, "seconds")

            await mongoHandler.sort_all_stations()
            # await mongoHandler.view_all_stations()

        except Exception as e:
            print("Error: " + str(e))

if __name__ == '__main__':
    asyncio.run(main())
