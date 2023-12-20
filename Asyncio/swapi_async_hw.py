

import datetime
import asyncio
import aiohttp
from more_itertools import chunked

from models import Session, Base, SwapiPeople, engine




CHUNK_SIZE = 5

async def get_people(client, people_id):

    response = await client.get(f"https://swapi.dev/api/people/{people_id}")

    json_data = await response.json()

    return json_data



async def get_url_name(client, url_):
    response = await client.get(url_)
    json_data = await response.json()
    return json_data




async def insert_to_db(results):
    async with Session() as session:
        swapi_people_list = [SwapiPeople(json=item) for item in results]

        session.add_all(swapi_people_list)
        await session.commit()




async def main():

    # async with engine.begin() as con:
    #     await con.run_sync(Base.metadata.drop_all)

    async with engine.begin() as con:
        await con.run_sync(Base.metadata.create_all)

    async with aiohttp.ClientSession() as client:

        for ids_chunk in chunked(range(1, 84), CHUNK_SIZE):

            coros = [get_people(client, i) for i in ids_chunk]
            results = await asyncio.gather(*coros)

            result_new = results.copy()

            num = -1
            for result in results:
                num += 1

                # print()
                # print(result_new[num])

                print('name:', result['name'])


                coros_homeworld = get_url_name(client, result['homeworld'])
                res_homeworld = await asyncio.gather(coros_homeworld)
                result_new[num]['homeworld'] = res_homeworld[0]['name']



                j = -1
                for i in result['films']:

                    coros_ = get_url_name(client, i)
                    res = await asyncio.gather(coros_)
                    j += 1
                    result_new[num]['films'][j] = res[0]['title']



                j = -1
                for i in result['species']:
                    coros_ = get_url_name(client, i)
                    res = await asyncio.gather(coros_)
                    j += 1
                    result_new[num]['species'][j] = res[0]['name']

                j = -1
                for i in result['starships']:
                    coros_ = get_url_name(client, i)
                    res = await asyncio.gather(coros_)
                    j += 1
                    result_new[num]['starships'][j] = res[0]['name']


                j = -1
                for i in result['vehicles']:
                    coros_ = get_url_name(client, i)
                    res = await asyncio.gather(coros_)
                    j += 1
                    result_new[num]['vehicles'][j] = res[0]['name']



                # print(result_new[num])



            insert_to_db_coro = insert_to_db(result_new)

            asyncio.create_task(insert_to_db_coro)


    current_task = asyncio.current_task()

    tasks_to_await = asyncio.all_tasks() - {current_task, }

    for task in tasks_to_await:
        await task




if __name__ == '__main__':
    start = datetime.datetime.now()
    asyncio.run(main())
    print(datetime.datetime.now() - start)

