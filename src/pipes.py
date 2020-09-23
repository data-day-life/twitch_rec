import asyncio
from typing import List
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


    def input(self, q_in: asyncio.Queue = None) -> asyncio.Queue:
        if self._q_in is None and isinstance(q_in, asyncio.Queue):
            self._q_in = q_in
        return self._q_in


    def output(self, q_out: asyncio.Queue = None) -> asyncio.Queue:
        if self._q_out is None and isinstance(q_out, asyncio.Queue):
            self._q_out = q_out
        return self._q_out


    async def produce(self, tc: TwitchClient):
        raise NotImplementedError


    async def create_tasks(self, tc: TwitchClient):
        raise NotImplementedError



class Pipeline:
    pipes: List[Pipe] = []
    tasks: List[asyncio.Task] = []


    def __init__(self, tc: TwitchClient):
        self.tc = tc
        pass


    def add_pipe(self, new_pipe: Pipe):
        self.pipes.append(new_pipe)
        self.tasks.extend(new_pipe.tasks)
        self._link_queues(new_pipe)


    def _link_queues(self, new_pipe: Pipe):
        if len(self.pipes) >= 2:
            new_pipe.input(self.pipes[-2].output())



    async def create_all_tasks(self):
        [await pipe.create_tasks(self.tc) for pipe in self.pipes]


    def cancel_all_tasks(self):
        [t.cancel() for t in self.tasks]


    async def run(self):
        if self.tasks:
            await asyncio.gather(self.tasks[0])
        if len(self.tasks) > 1:
            [await pipe.output().join() for pipe in self.pipes]
        self.cancel_all_tasks()


def main():
    foo = Pipe()
    print('')
    pass


if __name__ == "__main__":
    main()

