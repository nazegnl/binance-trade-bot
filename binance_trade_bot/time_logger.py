import time


class TimeLogger:
    def __init__(self, action) -> None:
        self.action = action
        self.start = None

    def __enter__(self) -> None:
        self.start = time.time()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.log("Processing Time - {:6.0f}ms - {}".format((time.time() - self.start) * 1000.0, self.action))

    @staticmethod
    def log(msg) -> None:
        print(msg)


def stopwatch(func):
    def decorated(*args, **kwargs):
        method_class = args[0]
        action = f"{method_class.__class__.__name__}.{func.__name__}"
        # for item in args[1:]:
        #     action += u".{}".format(item)
        with TimeLogger(action):
            return func(*args, **kwargs)

    return decorated
