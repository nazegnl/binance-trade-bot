import math
import time
from typing import Any, Dict, List, Optional, Tuple, Union

from binance.client import Client
from binance.exceptions import BinanceAPIException
from cachetools import TTLCache, cached

from .config import Config
from .database import Database
from .logger import Logger
from .models import Coin


class AllTickers:  # pylint: disable=too-few-public-methods
    def __init__(self, all_tickers: List[Dict]):
        self.all_tickers = all_tickers

    def get_price(self, ticker_symbol):
        ticker = next((t for t in self.all_tickers if t["symbol"] == ticker_symbol), None)
        return float(ticker["price"]) if ticker else None


class BinanceAPIManager:
    def __init__(self, config: Config, db: Database, logger: Logger):
        self.binance_client = Client(
            config.BINANCE_API_KEY,
            config.BINANCE_API_SECRET_KEY,
            tld=config.BINANCE_TLD,
        )
        self.db = db
        self.logger = logger
        self.config = config

    @cached(cache=TTLCache(maxsize=1, ttl=43200))
    def get_trade_fees(self) -> Dict[str, float]:
        return {
            ticker["symbol"]: ticker["taker"] for ticker in self.retry(self.binance_client.get_trade_fee)["tradeFee"]
        }

    def get_fee(self):
        return 0.00075

    def get_all_market_tickers(self) -> AllTickers:
        """
        Get ticker price of all coins
        """
        return AllTickers(self.retry(self.binance_client.get_all_tickers))

    def get_market_ticker_price(self, ticker_symbol: str):
        """
        Get ticker price of a specific coin
        """
        price = self.retry(self.binance_client.get_symbol_ticker, symbol=ticker_symbol)
        return float(price["price"]) if price else None

    def get_full_balance(self):
        """
        Get full balance of the current account
        """
        return {
            currency_balance["asset"]: float(currency_balance.get("free", 0))
            for currency_balance in self.retry(self.binance_client.get_account)["balances"]
            if currency_balance["asset"]
        }

    def get_currency_balance(self, currency_symbol: str):
        """
        Get balance of a specific coin
        """
        return self.get_full_balance().get(currency_symbol)

    def retry(self, func, *args, **kwargs):
        attempts = 0
        timeout = 0.1
        while attempts < 20:
            try:
                return func(*args, **kwargs)
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

    def get_symbol_filter(self, origin_symbol: str, target_symbol: str, filter_type: str):
        return next(
            _filter
            for _filter in self.get_symbol_info(origin_symbol + target_symbol)["filters"]
            if _filter["filterType"] == filter_type
        )

    @cached(cache=TTLCache(maxsize=2000, ttl=43200))
    def get_symbol_info(self, origin_symbol: str, target_symbol: str):
        return self.retry(self.binance_client.get_symbol_info, origin_symbol + target_symbol)

    def get_alt_tick(self, origin_symbol: str, target_symbol: str):
        step_size = self.get_symbol_filter(origin_symbol, target_symbol, "LOT_SIZE")["stepSize"]
        if step_size.find("1") == 0:
            return 1 - step_size.find(".")
        return step_size.find("1") - 1

    def get_min_notional(self, origin_symbol: str, target_symbol: str):
        return float(self.get_symbol_filter(origin_symbol, target_symbol, "MIN_NOTIONAL")["minNotional"])

    def wait_for_order(self, origin_symbol, target_symbol, order_id) -> Optional[Any]:
        while True:
            order_status = self.retry(
                self.binance_client.get_order, symbol=origin_symbol + target_symbol, orderId=order_id
            )
            self.logger.info(order_status)
            if order_status["status"] == "FILLED":
                return order_status

            if self._should_cancel_order(order_status):
                cancel_order = None
                while cancel_order is None:
                    cancel_order = self.retry(
                        self.binance_client.cancel_order, symbol=origin_symbol + target_symbol, orderId=order_id
                    )

                self.logger.info("Order timeout, canceled...")

                # sell partially
                if order_status["status"] == "PARTIALLY_FILLED" and order_status["side"] == "BUY":
                    self.logger.info("Sell partially filled amount")

                    order_quantity = self._sell_quantity(origin_symbol, target_symbol)
                    partially_order = None
                    while partially_order is None:
                        partially_order = self.retry(
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


    def _should_cancel_order(self, order_status):
        minutes = (time.time() - order_status["time"] / 1000) / 60
        timeout = float(self.config.SELL_TIMEOUT) if order_status["side"] == "SELL" else float(self.config.BUY_TIMEOUT)

        if timeout and minutes > timeout and order_status["status"] == "NEW":
            return True

        if timeout and minutes > timeout and order_status["status"] == "PARTIALLY_FILLED":
            if order_status["side"] == "SELL":
                return True

            if order_status["side"] == "BUY":
                current_price = self.get_market_ticker_price(order_status["symbol"])
                if float(current_price) * (1 - 0.00075) > float(order_status["price"]):
                    return True

        return False

    def _buy_quantity(
        self, origin_symbol: str, target_symbol: str, target_balance: float = None, from_coin_price: float = None
    ):
        target_balance = target_balance or self.get_currency_balance(target_symbol)
        from_coin_price = from_coin_price or self.get_all_market_tickers().get_price(origin_symbol + target_symbol)

        origin_tick = self.get_alt_tick(origin_symbol, target_symbol)
        return math.floor(target_balance * 10 ** origin_tick / from_coin_price) / float(10 ** origin_tick)

    def buy_alt(
        self, origin_coin: Coin, target_coin: Coin, all_tickers: AllTickers
    ) -> Union[Tuple[bool, None], Tuple[bool, dict]]:
        """
        Buy altcoin
        """
        origin_symbol = origin_coin.symbol
        target_symbol = target_coin.symbol

        full_balance = self.get_full_balance()
        origin_balance = full_balance.get(origin_symbol)
        target_balance = full_balance.get(target_symbol)
        from_coin_price = all_tickers.get_price(origin_symbol + target_symbol)

        order_quantity = self._buy_quantity(origin_symbol, target_symbol, target_balance, from_coin_price)

        if order_quantity * from_coin_price < self.get_min_notional(origin_symbol, target_symbol):
            return False, None

        self.logger.info(f"Buying {order_quantity} of {origin_symbol} with price {from_coin_price:>6.9f}")

        trade_log = self.db.start_trade_log(origin_coin, target_coin, False)

        order = self.retry(
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
            stat = self.wait_for_order(origin_symbol, target_symbol, order["orderId"])
            if stat is None:
                trade_log.set_canceled()
                return True, None
            qty = stat["cummulativeQuoteQty"]

        self.logger.info(f"Bought {origin_symbol}")
        trade_log.set_complete(qty)

        return True, order

    def _sell_quantity(self, origin_symbol: str, target_symbol: str, origin_balance: float = None):
        origin_balance = origin_balance or self.get_currency_balance(origin_symbol)

        origin_tick = self.get_alt_tick(origin_symbol, target_symbol)
        return math.floor(origin_balance * 10 ** origin_tick) / float(10 ** origin_tick)

    def sell_alt(
        self, origin_coin: Coin, target_coin: Coin, all_tickers: AllTickers
    ) -> Union[Tuple[bool, None], Tuple[bool, dict]]:
        """
        Sell altcoin
        """
        origin_symbol = origin_coin.symbol
        target_symbol = target_coin.symbol

        full_balance = self.get_full_balance()
        origin_balance = full_balance.get(origin_symbol)
        target_balance = full_balance.get(target_symbol)
        from_coin_price = all_tickers.get_price(origin_symbol + target_symbol)

        order_quantity = self._sell_quantity(origin_symbol, target_symbol, origin_balance)

        if order_quantity * from_coin_price < self.get_min_notional(origin_symbol, target_symbol):
            return False, None

        self.logger.info(
            f"Selling {order_quantity} / {origin_balance} of {origin_symbol} with price {from_coin_price:>6.9f}"
        )

        trade_log = self.db.start_trade_log(origin_coin, target_coin, True)

        order = self.retry(
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
            stat = self.wait_for_order(origin_symbol, target_symbol, order["orderId"])
            if stat is None:
                trade_log.set_canceled()
                return True, None
            qty = stat["cummulativeQuoteQty"]

        self.logger.info(f"Sold {origin_symbol}")

        trade_log.set_complete(qty)

        return True, order
