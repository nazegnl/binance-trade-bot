#!python3
import os

from apscheduler.schedulers.blocking import BlockingScheduler

from .binance_api_manager import BinanceAPIManager
from .config import Config
from .database import Database
from .logger import Logger
from .maintenance import Maintenance
from .strategies import get_strategy


def main():
    logger = Logger()
    logger.info("Starting")

    config = Config()
    db = Database(logger, config)
    manager = BinanceAPIManager(config, db, logger)
    maintenance = Maintenance(manager, db, config, logger)

    strategy = get_strategy(config.STRATEGY)
    if strategy is None:
        logger.error("Invalid strategy name")
        return
    trader = strategy(manager, db, logger, config)
    logger.info(f"Chosen strategy: {config.STRATEGY}")

    logger.info("Creating database schema if it doesn't already exist")
    db.create_database()

    db.set_coins(config.SUPPORTED_COIN_LIST)
    db.migrate_old_state()

    current_coin = db.get_current_coin()
    if current_coin:
        logger.info(f"Current coin: {current_coin}")

    maintenance.warmup_cache()

    trader.initialize()

    scheduler = BlockingScheduler()
    scheduler.add_job(trader.scout, "interval", seconds=config.SCOUT_SLEEP_TIME)
    scheduler.add_job(trader.update_values, "interval", minutes=1)
    scheduler.add_job(db.prune_scout_history, "interval", minutes=1)
    scheduler.add_job(db.prune_value_history, "interval", hours=1)
    scheduler.add_job(maintenance.warmup_cache, "interval", hours=1)

    print("Press Ctrl+{} to exit".format("Break" if os.name == "nt" else "C"))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
