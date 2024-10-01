import asyncio
import logging

import binance.websockets


async def coro(time, payload):
    print(payload)

async def main():
    print("Starting")
    logging.basicConfig(level=logging.DEBUG)
    url = "wss://stream.binance.com:9443/"
    path = 'streams=usdcusdt@aggTrade/bnbusdt@aggTrade/btcusdt@aggTrade'
    conn = binance.websockets.ReconnectingWebsocket(asyncio.get_event_loop(), path, coro, url, prefix = 'stream?')
    # path = 'btcusdt@aggTrade'
    # conn = binance.websockets.ReconnectingWebsocket(asyncio.get_event_loop(), path, coro, url, prefix = 'ws/')
    await asyncio.sleep(15)

if __name__ == "__main__":
    asyncio.run(main())
