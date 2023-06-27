from pyquokka.dataset.base_dataset import *

# this dataset will generate a sequence of numbers, from 0 to limit. 
class InputRestGetAPIDataset:
    def __init__(self, url, arguments, headers, projection, batch_size = 10000) -> None:
        
        self.url = url
        self.headers = headers
        self.arguments = arguments
        self.projection = projection

        # how many requests each execute will fetch.
        self.batch_size = batch_size

    def get_own_state(self, num_channels):
        channel_info = {}

        # stride the arguments across the channels in chunks of batch_size
        for channel in range(num_channels):
            channel_info[channel] = [(i, i+self.batch_size) for i in range(channel * self.batch_size, len(self.arguments), num_channels*self.batch_size)]
        print(channel_info)
        return channel_info

    def execute(self, channel, state = None):
        import aiohttp
        async def get(url, session):
            try:
                async with session.get(url=self.url + url, headers = self.headers) as response:
                    resp = await response.json()
                    for projection in self.projection:
                        if projection not in resp:
                            resp[projection] = None
                    return resp
            except Exception as e:
                print("Unable to get url {} due to {}.".format(url, e.__class__))

        async def main(urls):
            async with aiohttp.ClientSession() as session:
                ret = await asyncio.gather(*[get(url, session) for url in urls])
            return ret

        results = asyncio.run(main(self.arguments[state[0]: state[1]]))
        # find the first non-None result
        results = [i for i in results if i is not None] 

        return None, polars.from_records(results).select(self.projection)

class InputRestPostAPIDataset:
    def __init__(self, url, arguments, headers, projection, batch_size = 1000) -> None:
        
        self.url = url
        self.headers = headers
        self.arguments = arguments
        self.projection = projection
        self.batch_size = batch_size

    def get_own_state(self, num_channels):
        channel_info = {}

        # stride the arguments across the channels in chunks of batch_size
        for channel in range(num_channels):
            channel_info[channel] = [(i, i+self.batch_size) for i in range(channel * self.batch_size, len(self.arguments), num_channels*self.batch_size)]
        print(channel_info)
        return channel_info

    def execute(self, channel, state = None):
        import aiohttp
        async def get(data, session):
            try:
                async with session.post(url=self.url, data = json.dumps(data), headers = self.headers) as response:
                    resp = await response.json()
                    return polars.from_records(resp['result'])
            except Exception as e:
                print("Unable to post url {} payload {} headers {} due to {}.".format(self.url, json.dumps(data), json.dumps(self.headers), e.__class__))

        async def main(datas):
            async with aiohttp.ClientSession() as session:
                ret = await asyncio.gather(*[get(data, session) for data in datas])
            return ret
        
        results = asyncio.run(main(self.arguments[state[0]: state[1]]))
        # find the first non-None result
        results = polars.concat([i for i in results if i is not None]).select(self.projection)
        return None, results