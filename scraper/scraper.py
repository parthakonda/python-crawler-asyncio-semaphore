import logging
from bs4 import BeautifulSoup
import asyncio
from aiohttp_requests import requests
import settings


class Scraper(object):

    def __init__(self, urls=None):
        assert urls is not None, "urls should not be None"
        assert isinstance(urls, list), "urls should be list"
        self.urls = urls

    async def fetch(self, url, mySemaphore):
        await mySemaphore.acquire()
        assert url is not None, "url should not be None"
        assert isinstance(url, str), "url should be string"
        print(url)
        response = await requests.get(url)
        html = await response.text()
        data = await self.parse(url, html)
        mySemaphore.release()
        return data

    async def parse(self, url, html=None):
        assert html is not None, "html should not be None"
        assert isinstance(html, str), "html should be string"
        rows = []
        soup = BeautifulSoup(html, 'html.parser')
        for row in soup.select('tr,tr[height="50"]'):
            row_data = {}
            for col_idx, col in enumerate(row.select('td')):
                row_data.update({
                    settings.HEADER[col_idx]: col.text
                })
            rows.append(row_data)
        day = url.split("=")[1]
        return rows

    async def scrape(self):
        mySemaphore = asyncio.Semaphore(value=3)
        tasks = []
        for url in self.urls:
            tasks.append(self.fetch(url, mySemaphore))
        data = await asyncio.gather(*tasks)
        return data


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    scraper = Scraper(urls=settings.URLS)
    loop.run_until_complete(scraper.scrape())
