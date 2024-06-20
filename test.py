import asyncio
import json
import math

from binance import AsyncClient, DepthCacheManager, BinanceSocketManager


async def main():
    client = await AsyncClient.create()
    # print(json.dumps(await client.get_exchange_info(), indent=2))
    # print(json.dumps(await client.get_symbol_ticker(symbol="BTCUSDT"), indent=2))
    bsm = BinanceSocketManager(client, loop)
    async def handle_evt(msg):
        pair = msg['s']
        print(f'{pair} : {msg}')

    # create listener, can use the `ethkey` value to close the socket later
    trxkey = await bsm.start_binary_aggtrade_socket('BTCUSDT', handle_evt)
    await asyncio.sleep(math.inf)


if __name__ == "__main__":

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
