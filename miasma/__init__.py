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
        logger.log(logging.INFO, "%s%s%s", level, ' ' if level else '', self)
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
            logger.log(logging.DEBUG, "%s%sDONE %s", level, ' ' if level else '', self)
            return e.value

    async def __call__(self, retry=0, level=""):
        tried = 0
        while self.retry and tried < retry:
            tried += 1
            try:
                self.set_result(await self.__run__(retry, f"{level}({tried})"))
            except Exception as e:
                logger.exception("%s FAILED %s", f"{level}({tried})", self)
                continue
            else:
                return self.result()

        if tried:
            level = f"{level}({tried+1})"

        try:
            self.set_result(await self.__run__(retry, level))
        except Exception as e:
            logger.log(logging.ERROR, "%s%sFAILED %s", level, ' ' if level else '', self)
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


class Command:

    def __init__(self, *args, **kwargs):
        parser = ArgumentParser(*args, **kwargs)
        self._parser = parser
        self._subparsers = parser.add_subparsers(dest="COMMAND")
        self._commands = {}
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
