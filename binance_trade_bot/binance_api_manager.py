import dataclasses
import math
import time
from typing import Any, Dict, List, Optional, Union

from aiocache import cached
from attr import dataclass
from binance import AsyncClient
from binance.exceptions import BinanceAPIException

from .config import Config
from .database import Database
from .logger import Logger
from .models import Coin


class AllTickers:  # pylint: disable=too-few-public-methods
    def __init__(self, all_tickers: List[Dict]) -> None:
        self.all_tickers = all_tickers

    async def get_price(self, ticker_symbol) -> float:
        ticker = next((t for t in self.all_tickers if t["symbol"] == ticker_symbol), None)
        return float(ticker["price"]) if ticker else None


class BinanceAPIManager:
    binance_client: AsyncClient
    db: Database
    logger: Logger
    config: Config

    def __init__(self, config: Config, db: Database, logger: Logger):
        self.db = db
        self.logger = logger
        self.config = config
        self.binance_client = AsyncClient(
            self.config.BINANCE_API_KEY, self.config.BINANCE_API_SECRET_KEY, tld=self.config.BINANCE_TLD
        )

    @cached(ttl=43200)
    async def get_trade_fees(self) -> Dict[str, float]:
        fees = await self.retry(self.binance_client.get_trade_fee)
        return {ticker["symbol"]: ticker["taker"] for ticker in fees["tradeFee"]}

    def get_fee(self) -> float:
        return 0.00075

    async def get_all_market_tickers(self) -> AllTickers:
        """
        Get ticker price of all coins
        """
        tickers = await self.retry(self.binance_client.get_all_tickers)
        return AllTickers(tickers)

    async def get_market_ticker_price(self, ticker_symbol: str) -> float:
        """
        Get ticker price of a specific coin
        """
        price = await self.retry(self.binance_client.get_symbol_ticker, symbol=ticker_symbol)
        return float(price["price"]) if price else None

    async def get_full_balance(self) -> Dict[str, float]:
        """
        Get full balance of the current account
        """
        balances = await self.retry(self.binance_client.get_account)
        return {
            currency_balance["asset"]: float(currency_balance.get("free", 0))
            for currency_balance in balances["balances"]
            if currency_balance["asset"]
        }

    async def get_currency_balance(self, currency_symbol: str) -> float:
        """
        Get balance of a specific coin
        """
        balance = await self.get_full_balance()
        return balance.get(currency_symbol)

    async def retry(self, func, *args, **kwargs):
        attempts = 0
        timeout = 0.1
        while attempts < 20:
            try:
                return await func(*args, **kwargs)
            except BinanceAPIException as e:
                self.logger.info(e)
                time.sleep(timeout * 2 ** attempts)
            except Exception as e:  # pylint: disable=broad-except
                self.logger.info("Failed to execute binance api call.")
                if attempts == 0:
                    self.logger.info(e)
                attempts += 1
                time.sleep(1)
        return None

    async def get_symbol_filter(self, origin_symbol: str, target_symbol: str, filter_type: str) -> Dict:
        symbol_info = await self.get_symbol_info(origin_symbol, target_symbol)
        return next(_filter for _filter in symbol_info["filters"] if _filter["filterType"] == filter_type)

    @cached(ttl=43200)
    async def get_symbol_info(self, origin_symbol: str, target_symbol: str) -> Optional[Any]:
        return await self.retry(self.binance_client.get_symbol_info, origin_symbol + target_symbol)

    async def get_alt_tick(self, origin_symbol: str, target_symbol: str) -> float:
        symbol_filter = await self.get_symbol_filter(origin_symbol, target_symbol, "LOT_SIZE")
        if symbol_filter["stepSize"].find("1") == 0:
            return 1 - symbol_filter["stepSize"].find(".")
        return symbol_filter["stepSize"].find("1") - 1

    async def get_min_notional(self, origin_symbol: str, target_symbol: str) -> float:
        symbol_filter = await self.get_symbol_filter(origin_symbol, target_symbol, "MIN_NOTIONAL")
        return float(symbol_filter["minNotional"])

    async def wait_for_order(self, origin_symbol, target_symbol, order_id) -> Optional[Any]:
        while True:
            order_status = await self.retry(
                self.binance_client.get_order, symbol=origin_symbol + target_symbol, orderId=order_id
            )
            self.logger.info(order_status)
            if order_status["status"] == "FILLED":
                return order_status

            if self._should_cancel_order(order_status):
                cancel_order = None
                while cancel_order is None:
                    cancel_order = await self.retry(
                        self.binance_client.cancel_order, symbol=origin_symbol + target_symbol, orderId=order_id
                    )

                self.logger.info("Order timeout, canceled...")

                # sell partially
                if order_status["status"] == "PARTIALLY_FILLED" and order_status["side"] == "BUY":
                    self.logger.info("Sell partially filled amount")

                    order_quantity = await self._sell_quantity(origin_symbol, target_symbol)
                    partially_order = None
                    while partially_order is None:
                        partially_order = await self.retry(
                            self.binance_client.order_market_sell,
                            symbol=origin_symbol + target_symbol,
                            quantity=order_quantity,
                        )

                self.logger.info("Going back to scouting mode...")
                return None

            if order_status["status"] == "CANCELED":
                self.logger.info("Order is canceled")
                return None

            time.sleep(1)

    async def _should_cancel_order(self, order_status) -> bool:
        minutes = (time.time() - order_status["time"] / 1000) / 60
        timeout = float(self.config.SELL_TIMEOUT) if order_status["side"] == "SELL" else float(self.config.BUY_TIMEOUT)

        if timeout and minutes > timeout and order_status["status"] == "NEW":
            return True

        if timeout and minutes > timeout and order_status["status"] == "PARTIALLY_FILLED":
            if order_status["side"] == "SELL":
                return True

            if order_status["side"] == "BUY":
                current_price = await self.get_market_ticker_price(order_status["symbol"])
                if float(current_price) * (1 - 0.00075) > float(order_status["price"]):
                    return True

        return False

    async def _buy_quantity(
        self, origin_symbol: str, target_symbol: str, target_balance: float, from_coin_price: float
    ) -> float:
        origin_tick = await self.get_alt_tick(origin_symbol, target_symbol)
        return math.floor(target_balance * 10 ** origin_tick / from_coin_price) / float(10 ** origin_tick)

    async def buy_alt(
        self, origin_coin: Coin, target_coin: Coin, all_tickers: AllTickers
    ) -> Union[Tuple[bool, None], Tuple[bool, dict]]:
        """
        Buy altcoin
        """
        origin_symbol = origin_coin.symbol
        target_symbol = target_coin.symbol

        full_balance = await self.get_full_balance()
        origin_balance = full_balance.get(origin_symbol)
        target_balance = full_balance.get(target_symbol)
        from_coin_price = await all_tickers.get_price(origin_symbol + target_symbol)

        order_quantity = await self._buy_quantity(origin_symbol, target_symbol, target_balance, from_coin_price)

        if order_quantity * from_coin_price < await self.get_min_notional(origin_symbol, target_symbol):
            return False, None

        self.logger.info(f"Buying {order_quantity} of {origin_symbol} with price {from_coin_price:>6.9f}")

        trade_log = self.db.start_trade_log(origin_coin, target_coin, False)

        order = await self.retry(
            self.binance_client.order_limit_buy,
            symbol=origin_symbol + target_symbol,
            quantity=order_quantity,
            price=from_coin_price,
        )

        if not order:
            trade_log.set_failed()
            return True, None

        self.logger.info(order)
        qty = order["executedQty"]
        if order["status"] != "FILLED":
            trade_log.set_ordered(origin_balance, target_balance, order_quantity)
            stat = await self.wait_for_order(origin_symbol, target_symbol, order["orderId"])
            if stat is None:
                trade_log.set_canceled()
                return True, None
            qty = stat["cummulativeQuoteQty"]

        self.logger.info(f"Bought {origin_symbol}")
        trade_log.set_complete(qty)

        return True, order

    async def _sell_quantity(self, origin_symbol: str, target_symbol: str, origin_balance: float) -> float:
        origin_tick = await self.get_alt_tick(origin_symbol, target_symbol)
        return math.floor(origin_balance * 10 ** origin_tick) / float(10 ** origin_tick)

    async def sell_alt(
        self, origin_coin: Coin, target_coin: Coin, all_tickers: AllTickers
    ) -> Union[Tuple[bool, None], Tuple[bool, dict]]:
        """
        Sell altcoin
        """
        origin_symbol = origin_coin.symbol
        target_symbol = target_coin.symbol

        full_balance = await self.get_full_balance()
        origin_balance = full_balance.get(origin_symbol)
        target_balance = full_balance.get(target_symbol)
        from_coin_price = await all_tickers.get_price(origin_symbol + target_symbol)

        order_quantity = await self._sell_quantity(origin_symbol, target_symbol, origin_balance)

        if order_quantity * from_coin_price < await self.get_min_notional(origin_symbol, target_symbol):
            return False, None

        self.logger.info(
            f"Selling {order_quantity} / {origin_balance} of {origin_symbol} with price {from_coin_price:>6.9f}"
        )

        trade_log = self.db.start_trade_log(origin_coin, target_coin, True)

        order = await self.retry(
            self.binance_client.order_limit_sell,
            symbol=origin_symbol + target_symbol,
            quantity=order_quantity,
            price=from_coin_price,
        )

        if not order:
            trade_log.set_failed()
            return True, None

        self.logger.info(order)

        qty = order["executedQty"]
        if order["status"] != "FILLED":
            trade_log.set_ordered(origin_balance, target_balance, order_quantity)
            stat = await self.wait_for_order(origin_symbol, target_symbol, order["orderId"])
            if stat is None:
                trade_log.set_canceled()
                return True, None
            qty = stat["cummulativeQuoteQty"]

        self.logger.info(f"Sold {origin_symbol}")

        trade_log.set_complete(qty)

        return True, order
