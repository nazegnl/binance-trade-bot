import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import List, Optional, Union

from sqlalchemy import delete, desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import Session

from .config import Config
from .logger import Logger
from .models import *  # pylint: disable=wildcard-import


class ScoutLog:
    def __init__(self, pair: Pair, target_ratio: float, current_coin_price: float, other_coin_price: float):
        self.target_ratio = target_ratio
        self.pair = pair
        self.current_coin_price = current_coin_price
        self.other_coin_price = other_coin_price


class Database:
    def __init__(self, logger: Logger, config: Config) -> None:
        self.logger = logger
        self.config = config
        self.engine = create_async_engine(config.DATABASE_CONNECTION, future=True)

    @asynccontextmanager
    async def db_session(self) -> Session:
        """
        Creates a context with an open SQLAlchemy session.
        """
        session: AsyncSession = AsyncSession(bind=self.engine, expire_on_commit=False)

        try:
            yield session
            await session.commit()
        except:
            await session.rollback()
            raise
        finally:
            await session.close()

    async def set_coins(self, symbols: List[str]) -> None:
        session: AsyncSession

        # Add coins to the database and set them as enabled or not
        async with self.db_session() as session:
            # For all the coins in the database, if the symbol no longer appears
            # in the config file, set the coin as disabled
            result = await session.execute(select(Coin))
            coins = result.scalars().all()
            for coin in coins:
                if coin.symbol not in symbols:
                    coin.enabled = False

            # For all the symbols in the config file, add them to the database
            # if they don't exist
            for symbol in symbols:
                coin = next((coin for coin in coins if coin.symbol == symbol), None)
                if coin is None:
                    session.add(Coin(symbol))
                else:
                    coin.enabled = True

        # For all the combinations of coins in the database, add a pair to the database
        async with self.db_session() as session:
            # TODO: make join query to prevent use of single selects in loop.
            result = await session.execute(select(Coin).filter(Coin.enabled))
            coins: List[Coin] = result.scalars().all()
            for from_coin in coins:
                for to_coin in coins:
                    if from_coin != to_coin:

                        result = await session.execute(
                            select(Pair).filter(Pair.from_coin == from_coin, Pair.to_coin == to_coin)
                        )
                        pair = result.first()
                        if pair is None:
                            session.add(Pair(from_coin, to_coin))

    async def get_coins(self, only_enabled=True) -> List[Coin]:
        session: AsyncSession
        async with self.db_session() as session:
            query = select(Coin)
            if only_enabled:
                query = query.filter(Coin.enabled)

            result = await session.execute(query)
            coins = result.scalars().all()
            session.expunge_all()
            return coins

    async def get_coin(self, coin: Union[Coin, str]) -> Coin:
        if isinstance(coin, Coin):
            return coin
        session: AsyncSession
        async with self.db_session() as session:
            result = await session.get(Coin, coin)
            if result:
                session.expunge(result)
            return result

    async def set_current_coin(self, coin: Union[Coin, str]) -> None:
        coin = await self.get_coin(coin)
        session: AsyncSession
        async with self.db_session() as session:
            if isinstance(coin, Coin):
                coin = await session.merge(coin)
            cc = CurrentCoin(coin)
            session.add(cc)

    async def get_current_coin(self) -> Optional[Coin]:
        session: AsyncSession
        async with self.db_session() as session:
            result = await session.execute(select(CurrentCoin).order_by(desc(CurrentCoin.datetime)))
            current_coin: CurrentCoin = result.scalars().first()
            if current_coin is None:
                return None
            session.expunge(current_coin)
            return current_coin

    async def get_pair(self, from_coin: Union[Coin, str], to_coin: Union[Coin, str]):
        result = await asyncio.gather(self.get_coin(from_coin), self.get_coin(to_coin))
        session: AsyncSession
        async with self.db_session() as session:
            result = await session.execute(select(Pair).filter(Pair.from_coin == result[0], Pair.to_coin == result[1]))
            pair: Pair = result.first()
            session.expunge(pair)
            return pair

    async def get_pairs_from(self, from_coin: Union[Coin, str], only_enabled=True) -> List[Pair]:
        from_coin: Coin = await self.get_coin(from_coin)
        session: AsyncSession
        async with self.db_session() as session:
            pairs = select(Pair).filter(Pair.from_coin == from_coin)
            if only_enabled:
                pairs = pairs.filter(Pair.enabled.is_(True))
            result = await session.execute(pairs)
            pairs = result.scalars().all()
            session.expunge_all()
            return pairs

    async def get_pairs(self, only_enabled=True) -> List[Pair]:
        session: AsyncSession
        async with self.db_session() as session:
            pairs = select(Pair)
            if only_enabled:
                pairs = pairs.filter(Pair.enabled.is_(True))
            result = await session.execute(pairs)
            pairs = result.scalars().all()
            session.expunge_all()
            return pairs

    async def log_scout(self, scouts: List[ScoutLog]) -> None:
        session: AsyncSession
        async with self.db_session() as session:
            for log in scouts:
                merged_pair = await session.merge(log.pair)
                sh = ScoutHistory(merged_pair, log.target_ratio, log.current_coin_price, log.other_coin_price)
                session.add(sh)

    async def get_last_sell_trade(self) -> Optional[Trade]:
        session: AsyncSession
        async with self.db_session() as session:
            result = await session.execute(
                select(Trade).filter(Trade.selling, Trade.state == TradeState.COMPLETE).order_by(desc(Trade.datetime))
            )
            previous_sell_trade: Trade = result.first()
            if previous_sell_trade is None:
                return None
            session.expunge(previous_sell_trade)
            return previous_sell_trade

    async def prune_scout_history(self) -> None:
        time_diff: datetime = datetime.now() - timedelta(hours=self.config.SCOUT_HISTORY_PRUNE_TIME)
        session: AsyncSession
        async with self.db_session() as session:
            await session.execute(delete(ScoutHistory).filter(ScoutHistory.datetime < time_diff))

    async def prune_value_history(self) -> None:
        session: AsyncSession
        async with self.db_session() as session:
            # Sets the first entry for each coin for each hour as 'hourly'
            result = await session.execute(
                select(CoinValue).group_by(CoinValue.coin_id, func.strftime("%H", CoinValue.datetime))
            )
            hourly_entries: List[CoinValue] = result.scalars().all()
            for entry in hourly_entries:
                entry.interval = Interval.HOURLY

            # Sets the first entry for each coin for each day as 'daily'
            result = await session.execute(select(CoinValue).group_by(CoinValue.coin_id, func.date(CoinValue.datetime)))
            daily_entries: List[CoinValue] = result.scalars().all()
            for entry in daily_entries:
                entry.interval = Interval.DAILY

            # Sets the first entry for each coin for each month as 'weekly'
            # (Sunday is the start of the week)
            result = await session.execute(
                select(CoinValue).group_by(CoinValue.coin_id, func.strftime("%Y-%W", CoinValue.datetime))
            )
            weekly_entries: List[CoinValue] = result.scalars().all()
            for entry in weekly_entries:
                entry.interval = Interval.WEEKLY

            # The last 24 hours worth of minutely entries will be kept, so
            # count(coins) * 1440 entries
            time_diff = datetime.now() - timedelta(hours=24)
            await session.execute(
                delete(CoinValue).filter(CoinValue.interval == Interval.MINUTELY, CoinValue.datetime < time_diff)
            )

            # The last 28 days worth of hourly entries will be kept, so count(coins) * 672 entries
            time_diff = datetime.now() - timedelta(days=28)
            await session.execute(
                delete(CoinValue).filter(CoinValue.interval == Interval.HOURLY, CoinValue.datetime < time_diff)
            )

            # The last years worth of daily entries will be kept, so count(coins) * 365 entries
            time_diff = datetime.now() - timedelta(days=365)
            await session.execute(
                delete(CoinValue).filter(CoinValue.interval == Interval.DAILY, CoinValue.datetime < time_diff)
            )
            # All weekly entries will be kept forever

    async def create_database(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def start_trade_log(self, from_coin: Coin, to_coin: Coin, selling: bool):
        tradeLog = TradeLog(self)
        await trade.set_init(from_coin, to_coin, selling)
        return tradeLog


class TradeLog:
    def __init__(self, db: Database) -> None:
        self.db = db
        session: AsyncSession
        self.trade = None

    async def set_init(self, from_coin: Coin, to_coin: Coin, selling: bool):
        async with self.db.db_session() as session:
            from_coin = await session.merge(from_coin)
            to_coin = await session.merge(to_coin)
            self.trade = Trade(from_coin, to_coin, selling)
            session.add(self.trade)
            # Flush so that SQLAlchemy fills in the id column
            session.flush()

    async def set_ordered(self, alt_starting_balance, crypto_starting_balance, alt_trade_amount) -> None:
        session: AsyncSession
        async with self.db.db_session() as session:
            trade: Trade = await session.merge(self.trade)
            trade.alt_starting_balance = alt_starting_balance
            trade.alt_trade_amount = alt_trade_amount
            trade.crypto_starting_balance = crypto_starting_balance
            trade.state = TradeState.ORDERED

    async def set_complete(self, crypto_trade_amount) -> None:
        session: AsyncSession
        async with self.db.db_session() as session:
            trade: Trade = await session.merge(self.trade)
            trade.crypto_trade_amount = crypto_trade_amount
            trade.price = float(trade.crypto_trade_amount) / float(trade.alt_trade_amount)
            trade.state = TradeState.COMPLETE

    async def set_canceled(self) -> None:
        session: AsyncSession
        async with self.db.db_session() as session:
            trade: Trade = await session.merge(self.trade)
            trade.state = TradeState.CANCELED

    async def set_failed(self) -> None:
        session: AsyncSession
        async with self.db.db_session() as session:
            trade: Trade = await session.merge(self.trade)
            trade.state = TradeState.FAILED
