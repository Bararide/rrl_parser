import json
import time
import aiofiles

import motor.motor_asyncio as motor

class MongoHandler():
    def __init__(self):
        self.client = motor.AsyncIOMotorClient('mongodb://localhost:27017')
        self.db = self.client['logistics']
        self.stations = self.db['stations']
        self.trains = self.db['trains']
        self.companies = self.db['companies']
    
    async def add_station(self, station: json):
            try:
                existing_station = await self.stations.find_one({"title": station["title"]})
                if existing_station:
                    return
                
                await self.stations.insert_one(station, {"ordered": False})
            except Exception as e:
                print("Error adding station to database : " + str(e))

    async def add_train(self, train: json):
        try:
            await self.trains.insert_one(train, {"ordered": False})
        except Exception as e:
            print("Error adding station to database : " + str(e))

    async def add_company(self, company: json):
        try:
            await self.companies.insert_one(company, {"ordered": False})
        except Exception as e:
            print("Error adding company to database : " + str(e))

    async def sort_all_stations(self):
        try:
            await self.stations.find().sort("title", 1).to_list(length=None)
            await self.stations.create_index({"direction": 1}, {'unique': True})
        except Exception as e:
            print("Error sorting stations in the database: " + str(e))


    async def view_all_stations(self):
        try:
            start_time = time.time()

            cursor = await self.stations.find({}, {"_id": 0}).to_list(length=None)

            end_time = time.time()
            execution_time = end_time - start_time
            print("Query execution time:", execution_time, "seconds")

            start_time = time.time()

            cursor1 = await self.stations.find({"direction": "Сумское"}, {"_id": 0}).to_list(length=None)

            end_time = time.time()
            execution_time = end_time - start_time
            print("Query execution time:", execution_time, "seconds")

            start_time = time.time()

            trains_cursor = await self.trains.find({}, {"_id": 0}).to_list(length=None)

            end_time = time.time()
            execution_time = end_time - start_time
            print("Query execution time:", execution_time, "seconds")

            start_time = time.time()

            companies_cursor = await self.companies.find({}, {"_id": 0}).to_list(length=None)

            end_time = time.time()
            execution_time = end_time - start_time
            print("Query execution time:", execution_time, "seconds")

            async with aiofiles.open("stations.json", "w", encoding="utf-8") as file:
                await file.write(json.dumps(cursor, indent=4, ensure_ascii=False))

            async with aiofiles.open("trains.json", "w", encoding="utf-8") as file:
                await file.write(json.dumps(trains_cursor, indent=4, ensure_ascii=False))

            async with aiofiles.open("companies.json", "w", encoding="utf-8") as file:
                await file.write(json.dumps(companies_cursor, indent=4, ensure_ascii=False))

        except Exception as e:
            print("Error viewing stations in the database: " + str(e))

    async def clear_stations_collection(self):
        try:
            result = await self.stations.delete_many({})
            print(f"Deleted {result.deleted_count} documents from the 'stations' collection.")
        except Exception as e:
            print("Error clearing stations collection: " + str(e))

    async def clear_trains_collection(self):
        try:
            result = await self.trains.delete_many({})
            print(f"Deleted {result.deleted_count} documents from the 'trains' collection.")
        except Exception as e:
            print("Error clearing trains collection: " + str(e))

    async def clear_companies_collection(self):
        try:
            result = await self.companies.delete_many({})
            print(f"Deleted {result.deleted_count} documents from the 'companies' collection.")
        except Exception as e:
            print("Error clearing companies collection: " + str(e))


