import random
import sys

from binance_trade_bot.auto_trader import AutoTrader


class Strategy(AutoTrader):
    async def initialize(self):
        await super().initialize()
        await self.initialize_current_coin()

    async def scout(self):
        """
        Scout for potential jumps from the current coin to another coin
        """
        all_tickers = await self.manager.get_all_market_tickers()
        current_coin = await self.db.get_current_coin()

        if current_coin.symbol == self.config.BRIDGE_SYMBOL:
            trade = self.db.get_last_sell_trade()
            if not trade:
                self.logger.info("Skipping scouting... stuck on bridge coin")
                return

            current_coin = trade.alt_coin_id
            current_coin_price = trade.price
        else:
            current_coin_price = all_tickers.get_price(current_coin + self.config.BRIDGE)

        if current_coin_price is None:
            self.logger.info("Skipping scouting... current coin {} not found".format(current_coin + self.config.BRIDGE))
            return

        await self._jump_to_best_coin(current_coin, current_coin_price, all_tickers)

    async def bridge_scout(self):
        current_coin = await self.db.get_current_coin()
        if self.manager.get_currency_balance(current_coin.symbol) > self.manager.get_min_notional(
            current_coin.symbol, self.config.BRIDGE.symbol
        ):
            # Only scout if we don't have enough of the current coin
            return
        new_coin = await super().bridge_scout()
        if new_coin is not None:
            await self.db.set_current_coin(new_coin)

    async def initialize_current_coin(self):
        """
        Decide what is the current coin, and set it up in the DB.
        """
        if await self.db.get_current_coin() is None:
            current_coin_symbol = self.config.CURRENT_COIN_SYMBOL
            if not current_coin_symbol:
                current_coin_symbol = random.choice(self.config.SUPPORTED_COIN_LIST)

            self.logger.info(f"Setting initial coin to {current_coin_symbol}")

            if current_coin_symbol not in self.config.SUPPORTED_COIN_LIST:
                sys.exit("***\nERROR!\nSince there is no backup file, a proper coin name must be provided at init\n***")
            await self.db.set_current_coin(current_coin_symbol)

            # if we don't have a configuration, we selected a coin at random... Buy it so we can start trading.
            if self.config.CURRENT_COIN_SYMBOL == "":
                current_coin = await self.db.get_current_coin()
                self.logger.info(f"Purchasing {current_coin} to begin trading")
                all_tickers = await self.manager.get_all_market_tickers()
                await self.manager.buy_alt(current_coin, self.config.BRIDGE, all_tickers)
                self.logger.info("Ready to start trading")
