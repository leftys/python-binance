# coding=utf-8

import asyncio
import time
import logging
import sortedcontainers

from .websockets import BinanceSocketManager


class DepthCache(object):

    def __init__(self, symbol):
        """Initialise the DepthCache

        :param symbol: Symbol to create depth cache for
        :type symbol: string

        """
        self.symbol = symbol
        self._bids = sortedcontainers.SortedDict(lambda x: -x)
        self._asks = sortedcontainers.SortedDict()
        self.update_time = None

    def add_bid(self, bid):
        """Add a bid to the cache

        :param bid:
        :return:

        """
        self._bids[float(bid[0])] = float(bid[1])
        if bid[1] == "0.00000000":
            try:
                del self._bids[float(bid[0])]
            except KeyError:
                pass

    def add_ask(self, ask):
        """Add an ask to the cache

        :param ask:
        :return:

        """
        self._asks[float(ask[0])] = float(ask[1])
        if ask[1] == "0.00000000":
            try:
                del self._asks[float(ask[0])]
            except KeyError:
                pass

    def get_bids(self):
        """Get the current bids

        :return: list of bids with price and quantity as floats

        .. code-block:: python

            [
                [
                    0.0001946,  # Price
                    45.0        # Quantity
                ],
                [
                    0.00019459,
                    2384.0
                ],
                [
                    0.00019158,
                    5219.0
                ],
                [
                    0.00019157,
                    1180.0
                ],
                [
                    0.00019082,
                    287.0
                ]
            ]

        """
        return self._bids.items()

    def get_asks(self):
        """Get the current asks

        :return: list of asks with price and quantity as floats

        .. code-block:: python

            [
                [
                    0.0001955,  # Price
                    57.0        # Quantity
                ],
                [
                    0.00019699,
                    778.0
                ],
                [
                    0.000197,
                    64.0
                ],
                [
                    0.00019709,
                    1130.0
                ],
                [
                    0.0001971,
                    385.0
                ]
            ]

        """
        return self._asks.items()

    def clear(self) -> None:
        self._bids.clear()
        self._asks.clear()
        self.update_time = None


class DepthCacheManager(object):

    _default_refresh = 60 * 30  # 30 minutes

    @classmethod
    async def create(cls, client, loop, symbol, coro=None, refresh_interval=_default_refresh, bm=None, limit=500):
        """Create a DepthCacheManager instance

        :param client: Binance API client
        :type client: binance.Client
        :param loop:
        :type loop:
        :param symbol: Symbol to create depth cache for
        :type symbol: string
        :param coro: Optional coroutine to receive depth cache updates
        :type coro: async coroutine
        :param refresh_interval: Optional number of seconds between cache refresh, use 0 or None to disable
        :type refresh_interval: int
        :param bm: Optional BinanceSocketManager
        :type bm: BinanceSocketManager
        :param limit: Optional number of orders to get from orderbook
        :type limit: int

        """
        self = DepthCacheManager()
        self._client = client
        self._loop = loop
        self._symbol = symbol
        self._limit = limit
        self._coro = coro
        self._last_update_id = None
        # TODO deque
        self._depth_message_buffer = []
        self._bm = bm
        self._depth_cache = DepthCache(self._symbol)
        self._refresh_interval = refresh_interval
        self._first_update_after_snapshot = True
        self._logger = logging.getLogger(__name__)
        self.last_depth_message = None

        await self._start_socket()
        await self._init_cache()

        return self

    async def _init_cache(self):
        """Initialise the depth cache calling REST endpoint

        :return:
        """
        self._last_update_id = None
        self._depth_message_buffer = []
        self._first_update_after_snapshot = True

        # wait for some socket responses
        while not len(self._depth_message_buffer):
            await asyncio.sleep(0.02)
        await asyncio.sleep(0.1)

        res = await self._client.get_order_book(symbol=self._symbol, limit=self._limit)

        # process bid and asks from the order book
        self._depth_cache.clear()
        for bid in res['bids']:
            self._depth_cache.add_bid(bid)
        for ask in res['asks']:
            self._depth_cache.add_ask(ask)
        await asyncio.sleep(0)

        # set a time to refresh the depth cache
        if self._refresh_interval:
            self._refresh_time = int(time.time()) + self._refresh_interval

        # set first update id
        self._last_update_id = res['lastUpdateId']

        # Apply any updates from the websocket
        while self._depth_message_buffer:
            msg = self._depth_message_buffer.pop(0)
            await self._process_depth_message(msg, buffer=True)

    async def _start_socket(self):
        """Start the depth cache socket

        :return:
        """
        if self._bm is None:
            self._bm = BinanceSocketManager(self._client, self._loop)

        await self._bm.start_depth_socket(self._symbol, self._depth_event)

    async def _depth_event(self, msg):
        """Handle a depth event

        :param msg:
        :return:

        """
        self.last_depth_message = msg
        if 'e' in msg and msg['e'] == 'error':
            # close the socket
            await self.close()

            # notify the user by returning a None value
            if self._coro:
                await self._coro(None)

        if self._last_update_id is None or self._depth_message_buffer:
            # Initial depth snapshot fetch not yet performed, buffer messages
            self._depth_message_buffer.append(msg)
        else:
            await self._process_depth_message(msg)

    async def _process_depth_message(self, msg, buffer=False):
        """Process a depth event message.

        :param msg: Depth event message.
        :return:

        """
        if msg['U'] == self._last_update_id + 1:
            # Skip the rest for the most common success case
            pass
        elif msg['u'] < self._last_update_id - 10_000:
            # This should not happen!
            raise ValueError('Received super-old depth message', msg['u'], self._last_update_id, msg)
        elif buffer and msg['u'] <= self._last_update_id:
            # ignore any updates before the initial update id when initializing
            return
        elif self._first_update_after_snapshot and msg['u'] <= self._last_update_id:
            self._first_update_after_snapshot = False
            return
        elif msg['U'] <= self._last_update_id + 1 <= msg['u'] and self._first_update_after_snapshot:
            # apply the first partially applied update after snapshot
            pass
        elif msg['U'] != self._last_update_id + 1:
            # if not buffered check we get sequential updates
            # otherwise init cache again
            self._logger.warning(
                'Reiniting cache of %s because of non-consequential update id! Batch %d-%d followed %d',
                self._symbol,
                msg['U'],
                msg['u'],
                self._last_update_id,
            )
            self._last_update_id = None
            asyncio.ensure_future(self._init_cache())
        self._first_update_after_snapshot = False

        # add any bid or ask values
        for bid in msg['b']:
            self._depth_cache.add_bid(bid)
        for ask in msg['a']:
            self._depth_cache.add_ask(ask)

        # keeping update time
        self._depth_cache.update_time = msg['E']

        # call the callback with the updated depth cache
        if self._coro:
            await self._coro(self._depth_cache)

        self._last_update_id = msg['u']

        # after processing event see if we need to refresh the depth cache
        if self._refresh_interval and int(time.time()) > self._refresh_time:
            self._last_update_id = None
            asyncio.ensure_future(self._init_cache())

    def get_depth_cache(self):
        """Get the current depth cache

        :return: DepthCache object

        """
        return self._depth_cache

    async def close(self):
        """Close the open socket for this manager

        :return:
        """
        await self._bm.close()
        self._last_update_id = None
        self._depth_message_buffer.clear()
