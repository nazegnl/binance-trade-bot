from datetime import datetime

from binance_trade_bot import backtest

if __name__ == "__main__":
    history = []
    btc_value = 0
    bridge_value = 0
    for manager in backtest(datetime(2021, 5, 1), datetime(2021, 5, 29)):
        btc_value = manager.collate_coins("BTC")
        bridge_value = manager.collate_coins(manager.config.BRIDGE.symbol)
        history.append((btc_value, bridge_value))

    btc_diff = round((btc_value - history[0][0]) / history[0][0] * 100, 3)
    bridge_diff = round((bridge_value - history[0][1]) / history[0][1] * 100, 3)

    print("BTC VALUE:", btc_value, f"({btc_diff}%)")
    print(f"BRIDGE VALUE:", bridge_value, f"({bridge_diff}%)")
