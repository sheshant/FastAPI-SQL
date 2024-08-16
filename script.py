import asyncio
import time
import csv
import itertools
from aiohttp_retry import RetryClient, ExponentialRetry


async def fetch_url(client, data):
    url = "http://localhost:8000/items/bulk/"
    async with client.post(url, ssl=False, json=data) as response:
        return response.status == 200


async def fetch_with_retry():
    tasks = []
    chunk_size = 100000
    file_path = "/Users/sheshantsingh/Downloads/training_mini.tsv"
    retry_options = ExponentialRetry(attempts=5)
    async with RetryClient(raise_for_status=False, retry_options=retry_options) as client:
        with open(file_path, 'r') as file:
            csv_reader = csv.reader(file, delimiter='\t')
            header = next(csv_reader)  # Read the header

            while True:
                chunk = list(itertools.islice(csv_reader, chunk_size))
                if not chunk:
                    break
                wikipedia_data = [{key: value for key, value in zip(header, row)} for row in chunk]
                tasks.append(fetch_url(client=client, data=wikipedia_data))

        await asyncio.gather(*tasks)


if __name__ == "__main__":
    start_time = time.perf_counter()
    asyncio.run(fetch_with_retry())
    print(time.perf_counter() - start_time)
