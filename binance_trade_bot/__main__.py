from anyio import run

from .crypto_trading import main

if __name__ == "__main__":
    try:
        run(main)
    except KeyboardInterrupt:
        pass
