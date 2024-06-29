import asyncio
import json
import aiohttp

from typing import List
from datetime import datetime

from mongo_handler import MongoHandler
from env import APIKEY

class ParserHandler():
    def __init__(self, mongoHandler: MongoHandler, session: aiohttp.ClientSession):
        self.session = session
        self.url = "https://api.rasp.yandex.net/v3.0/"
        self.mongoHandler = mongoHandler
        self.company_codes = set()

    async def get_train_on_station_codes(self, codes: List[str]) -> None:
        try:
            current_datetime = datetime.now()
            formatted_date = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

            params = {
                "apikey": APIKEY,
                "format": "json",
                "lang": "ru_RU",
                "date": formatted_date,
                "station": ""
            }

            tasks = [self.fetch_train_data(params, code) for code in codes]
            await asyncio.gather(*tasks)
        except Exception as e:
            print("Get all trains error: " + str(e))

    async def get_all_companies(self) -> None:
        try:
            params = {
                "apikey": APIKEY,
                "format": "json",
                "lang": "ru_RU",
                "code": ""
            }

            tasks = [self.fetch_company_data(params, code) for code in self.company_codes]
            await asyncio.gather(*tasks)

        except Exception as e:
            print("Get all companies error: " + str(e))

    async def get_all_stations(self) -> None:
        try:
            params = {
                "apikey": APIKEY,
                "format": "json",
                "lang": "ru_RU",
            }

            async with self.session.get(self.url + "stations_list/?", params=params) as response:
                if response.status == 200:
                    content = await response.text()
                    data = json.loads(content)
                    if "countries" in data:
                        station_tasks = []
                        train_tasks = []
                        for item in data["countries"]:
                            for r in item["regions"]:
                                for s in r["settlements"]:
                                    if s["title"] != "":
                                        for st in s["stations"]:
                                            if st["transport_type"] == "train" and st["direction"] != "":
                                                station_tasks.append(self.mongoHandler.add_station(st))
                                                train_tasks.append(st["codes"]["yandex_code"])

                        await asyncio.gather(*station_tasks)
                        await self.get_train_on_station_codes(train_tasks)
                else:
                    print("Произошла ошибка при выполнении запроса.")
        except Exception as e:
            print("Get all station error: " + str(e))

    async def _fetch_train_data(self, params: dict, code: str) -> None:
        try:
            params["station"] = code
            async with self.session.get(self.url + "schedule/?", params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data is not None and data.get("date"):
                        await self.mongoHandler.add_train(data)

                        if "schedule" in data and data["schedule"] is not None:
                            for schedule in data["schedule"]:
                                if "thread" in schedule and schedule["thread"] is not None \
                                    and "carrier" in schedule["thread"] \
                                    and schedule["thread"]["carrier"] is not None \
                                    and "code" in schedule["thread"]["carrier"]:
                                    
                                    carrier_title = schedule["thread"]["carrier"]["code"]
                                    self.company_codes.add(carrier_title)
        except Exception as e:
            print("Fetch all trains error: " + str(e))

    async def _fetch_company_data(self, params: dict, code: str) -> None:
        try:
            params["code"] = code
            async with self.session.get(self.url + "carrier/?", params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data is not None:
                        await self.mongoHandler.add_company(data)
                    else:
                        print("Response JSON is None")
        except Exception as e:
            print("Fetch all companies error: " + str(e))