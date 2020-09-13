import asyncio
from typing import List
from dataclasses import dataclass
from twitch_client import TwitchClient

"""
!!! Full generic implementation(s) :
    https://lbolla.info/pipelines-in-python

Large curated list that is probably overkill and may contain a lot of "noise"
    https://github.com/pditommaso/awesome-pipeline

StackOverflow -- Using a Coroutine as a Decorator
    https://stackoverflow.com/questions/42043226/using-a-coroutine-as-decorator

StackOverflow -- Async decorator for generators & coroutines
    https://stackoverflow.com/questions/54712966/asynchronous-decorator-for-both-generators-and-coroutines
"""


################################################################################################
################################################################################################


class Pipe:
    _q_in: asyncio.Queue = None
    _q_out: asyncio.Queue = None
    tasks: List[asyncio.Task] = []
    # data: dataclass

    # def __init__(self, datacls_obj: dataclass = None, **kwargs):
    #     self.data = datacls_obj
    #     for kwarg, val in kwargs.items():
    #         self.kwarg = val
    #


    # @property
    # def display(self):
    #     return self.data.display


    def input(self, q_in: asyncio.Queue = None):
        if self._q_in is None and isinstance(q_in, asyncio.Queue):
            self._q_in = q_in
        return self._q_in


    def output(self, q_out: asyncio.Queue = None):
        if self._q_out is None and isinstance(q_out, asyncio.Queue):
            self._q_out = q_out
        return self._q_out


    async def produce(self, tc: TwitchClient):
        raise NotImplementedError


    async def task(self):
        raise NotImplementedError



class Pipeline:
    pipes: List[Pipe] = []
    tasks: List[asyncio.Task] = []

    def __init__(self):
        pass

    def extend_pipeline(self, new_pipe: Pipe):
        self.pipes.append(new_pipe)
        self.tasks.extend(new_pipe.tasks)
        self._link_queues(new_pipe)

    def _link_queues(self, new_pipe: Pipe):
        num_pipes = len(self.pipes)
        if num_pipes >= 2:
            new_pipe._q_in = self.pipes[-2]._q_out
            new_pipe.input(self.pipes[-2].output())

    def cancel_all_tasks(self):
        [t.cancel() for t in self.tasks]

    async def run(self):
        pass


def main():
    foo = Pipe()
    print('')
    pass


if __name__ == "__main__":
    main()

