import os
import sys
from types import ModuleType
from argparse import ArgumentParser
from inspect import signature, BoundArguments
from functools import wraps
from asyncio import Future
import logging
logger = logging.getLogger(__package__)


class Task(Future):

    def __init__(self, func, ba, format, retry):
        super().__init__()
        self.func = func
        self.ba = ba
        self.format = format
        self.retry = retry

    def __str__(self):
        ba = BoundArguments(self.ba._signature, self.ba.arguments)
        ba.apply_defaults()

        if self.format:
            format = self.format.format if isinstance(self.format, str) else self.format
            return format(**ba.arguments)
        return '{}({})'.format(
            self.func.__name__,
            ', '.join(
                f'{arg}={value!r}'
                for arg,value in ba.arguments.items())
        )

    async def __run__(self, retry, level):
        logger.log(logging.WARNING, "%.0s %s %s", "\u23F5", level, self)
        subtask = None
        number = 0
        try:
            coro = self.func(*self.ba.args, **self.ba.kwargs)
            while True:
                try:
                    if subtask is None:
                        result = None
                    else:
                        result = await subtask(retry, f"{level}.{number}")
                except BaseException as e:
                    subtask = coro.throw(e)
                else:
                    subtask = coro.send(result)
                number += 1
        except StopIteration as e:
            logger.log(logging.INFO, "%.0s %s %s", "DONE", level, self)
            return e.value

    async def __call__(self, retry=0, level=""):
        tried = 0
        while self.retry and tried < retry:
            tried += 1
            try:
                self.set_result(await self.__run__(retry, f"{level}({tried})"))
            except Exception as e:
                logger.exception("%.0s %s %s", "FAIL", f"{level}({tried})", self)
                continue
            else:
                return self.result()

        if tried:
            level = f"{level}({tried+1})"

        try:
            self.set_result(await self.__run__(retry, level))
        except Exception as e:
            logger.log(logging.ERROR, "%.0s %s %s", "FAIL", level, self)
            self.set_exception(e)

        return self.result()

    def run(self, retry=0):
        coro = self(retry)
        try:
            coro.send(None)
        except StopIteration as e:
            return e.value


def task(format=None, retry=False):
    def decorator(func):
        sig = signature(func)

        @wraps(func)
        def wrapper(*args, **kwargs):
            return Task(func, sig.bind(*args, **kwargs), format, retry)

        return wrapper
    return decorator


def Argument(*args, **kwargs):
    return lambda dest: lambda parser: parser.add_argument(*args, dest=dest, **kwargs)


def logging_color(level):
    if level >= logging.CRITICAL:
        return "31"
    elif level >= logging.ERROR:
        return "91"
    elif level >= logging.WARNING:
        return "33"
    elif level >= logging.INFO:
        return "92"
    else:
        return "94"


class Formatter(logging.Formatter):

    def formatMessage(self, record):
        if record.msg.startswith("%.0s"):
            if sys.stderr.isatty():
                color = logging_color(record.levelno)
                record.message = "\x1b[1m\x1b[{}m{:>12}\x1b[39m\x1b[0m{}".format(color, record.args[0], record.message)
            else:
                record.message = "{}{}".format(record.args[0], record.message)
        return super().formatMessage(record)


class Command:

    def __init__(self, *args, **kwargs):
        parser = ArgumentParser(*args, **kwargs)
        self._parser = parser
        self._subparsers = parser.add_subparsers(dest="COMMAND")
        self._commands = {}
        self.add_argument("--timestamps", action="store_true", default=False, help="show timestamp on each log line")
        self.add_argument("--debug", action="store_true", default=False, help="turn on debug logging")
        self.add_argument("--retry", type=int, help="max number of retries")
        self.add_argument("-v", "--verbose", action="store_true", default=False, help="verbose output")
        self(self.help)

    def add_argument(self, *args, **kwargs):
        self._parser.add_argument(*args, **kwargs)

    def __call__(self, func):
        name = func.__name__.lower().replace("_", "-")
        subparser = self._subparsers.add_parser(name, help=func.__doc__)
        params = signature(func).parameters

        dests = []
        for param in params.values():
            if param.annotation == param.empty:
                continue
            param.annotation(param.name)(subparser)
            dests.append(param.name)

        @wraps(func)
        def wrapper(args):
            return func(**{d:getattr(args, d) for d in dests if getattr(args, d) is not None})

        self._commands[name] = wrapper
        return func

    def parse(self, args=None, default="help"):
        args = self._parser.parse_args(args)
        return self._commands[args.COMMAND or default], args

    @task("Print help message")
    async def help(self):
        """print help message"""
        self._parser.print_help()

    def run(self, init_mod=None, argv=None):
        mod = ModuleType("__miasma__")
        mod.command = self
        mod.Argument = Argument
        mod.task = task
        mod.VERBOSE = os.environ.get("VERBOSE", "0").lower() in ("1", "on", "yes", "true")

        logging.captureWarnings(True)
        logger = logging.getLogger('')
        logger.setLevel(logging.INFO if mod.VERBOSE else logging.WARNING)
        handler = logging.StreamHandler()
        logger.addHandler(handler)

        @task(self._parser.description)
        async def main():
            if init_mod is not None:
                cmd, args = await init_mod(mod, argv or sys.argv[1:])
            else:
                cmd, args = self.parse(argv)

            retry = args.retry
            if retry is None:
                if 'MAX_RETRY' in os.environ:
                    retry = int(os.environ['MAX_RETRY'])
                else:
                    retry = getattr(mod, 'MAX_RETRY', 3)

            formatter = Formatter()
            if args.timestamps:
                formatter = Formatter(fmt='{asctime} {message}', style='{')
            handler.setFormatter(formatter)
            if args.verbose:
                mod.VERBOSE = True
                logger.setLevel(logging.INFO)
            if args.debug:
                logger.setLevel(logging.DEBUG)
            await cmd(args)(retry=retry)

        main().run(retry=3)
