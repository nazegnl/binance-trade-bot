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

    async def warmup_cache(self) -> None:
        self.logger.debug("Warming up cache")
        await self.api_manager.get_trade_fees()

        for coin in self.database.get_coins():
            await self.api_manager.get_symbol_info(coin.symbol + self.config.BRIDGE_SYMBOL)
            await self.api_manager.get_symbol_info(self.config.BRIDGE_SYMBOL + coin.symbol)

    def handle_wallet(self) -> None:
        # TODO: Buy bnb
        # TODO: safe guard profit
        pass

    def update_coin_list(self) -> None:
        # TODO: Try find correlating coins and update system accordingly
        pass
