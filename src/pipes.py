import asyncio
from typing import List
from dataclasses import dataclass

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
	q_in: asyncio.Queue = asyncio.Queue
	q_out: asyncio.Queue = asyncio.Queue
	tasks: List[asyncio.Task] = []
	data: dataclass
	
	def __init__(self, datacls_obj: dataclass):
		self.data = datacls_obj
	
	@property
	def display(self):
		return self.data.display
	
	def put_queue(self, item_list):
		if self.q_out:
			[self.q_out.put_nowait(item) for item in item_list]


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
			new_pipe.q_in = self.pipes[-2].q_out
	
	def cancel_all_tasks(self):
		[t.cancel() for t in self.tasks]
	
	async def run(self):
		pass


