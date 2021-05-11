from binance_trade_bot import BinanceAPIManager
from binance_trade_bot.config import Config
from binance_trade_bot.database import Database
from binance_trade_bot.logger import Logger


class Maintenance:
    def __init__(self, api_manager: BinanceAPIManager, database: Database, config: Config, logger: Logger) -> None:
        self.logger = logger
        self.config = config
        self.database = database
        self.api_manager = api_manager

    def warmup_cache(self) -> None:
        self.logger.debug("Warming up cache")
        self.api_manager.get_trade_fees()

        for coin in self.database.get_coins():
            self.api_manager.get_symbol_info(coin + self.config.BRIDGE_SYMBOL)
            self.api_manager.get_symbol_info(self.config.BRIDGE_SYMBOL + coin)

    def handle_wallet(self) -> None:
        # TODO: Buy bnb
        # TODO: safe guard profit
        pass
