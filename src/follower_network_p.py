import asyncio
from collections import Counter
from typing import Set
from time import perf_counter
from colors import Col
from twitch_client import TwitchClient
from streamer_p import Streamer
from pipes import Pipe


class FollowerNetwork(Pipe):
    BATCH_SZ:       int = 100
    n_consumers:    int = 100
    max_followings: int = 150
    min_mutual:     int = 3
    num_collected:  int = 0
    num_skipped:    int = 0
    batch_history:  Set[str] = set()


    def __init__(self, streamer: Streamer, **kwargs):
        self.streamer = streamer
        self.max_followings = kwargs.get('max_followings', None) or self.max_followings
        self.min_mutual = kwargs.get('min_mutual', None) or self.min_mutual
        self.n_consumers = kwargs.get('n_consumers', None) or self.n_consumers
        self._followings_counter = Counter()


    @property
    def followings_counter(self) -> Counter:
        self._followings_counter.pop(self.streamer.uid, None)
        return self._followings_counter


    @property
    def mutual_followings(self) -> dict:
        return {uid: count for uid, count in self.followings_counter.items() if count >= self.min_mutual}


    def display(self, result=''):
        result += f'{Col.green}>>>>> Pipe || Follower Network {Col.end}\n'
        result += f'{Col.white}   * Total Skipped: {self.num_skipped:>4}{Col.end}\n'
        result += f'{Col.white}   *    Total Kept: {self.num_collected:>4}{Col.end}\n'
        result += f'{Col.white}   *         Total: {self.num_skipped + self.num_collected:>4}{Col.end}\n'
        result += f'{Col.green} -> Followings Counter (sz={len(self.followings_counter)}){Col.end}\n'
        result += f'    {self.followings_counter}\n'
        result += f'{Col.green} -> Mutual Followings (sz={len(self.mutual_followings)}){Col.end}\n'
        result += f'    {self.mutual_followings}\n'
        result += f'{Col.green} -> Batch History (sz={len(self.batch_history)}){Col.end}\n'
        result += f'   {self.batch_history}\n'
        return print(result)


    async def create_tasks(self, tc):
        # TODO: needs a future or callback to process remaining batches
        self.tasks.extend([asyncio.create_task(self.produce(tc)) for _ in range(self.n_consumers)])


    async def produce(self, tc):
        await self.fetch_followings(tc)


    async def fetch_followings(self, tc):
        """
        Fetches a follower id from the queue and collects a list of uids that they are following provided that they
        are not following more than max_total_followings.
        """
        while True:
            follower_id = await self._q_in.get()
            followings_found = await tc.fetch_capped_followings(follower_id, self.max_followings)
            new_candidate_batch = self.update_followings(followings_found)
            if new_candidate_batch and self._q_out:
                self._q_out.put_nowait(new_candidate_batch)
            self._q_in.task_done()


    def update_followings(self, foll_data, remainder=False) -> list:
        if foll_data:
            self.followings_counter.update([following.get('to_id') for following in foll_data])
            self.num_collected += 1
            return self.new_candidate_batches(remainder)
        else:
            self.num_skipped += 1
            return []


    def new_candidate_batches(self, remainder=False) -> list:
        new_candidates = self.mutual_followings.keys() - self.batch_history
        batches = self.batchify(list(new_candidates), remainder)
        flat_candidates = batches
        if remainder and batches and isinstance(batches[0], list):
            flat_candidates = [uid for sublist in batches for uid in sublist]
        self.batch_history.update(flat_candidates)
        return batches


    def batchify(self, candidates, fetch_all=False):
        result = []
        if fetch_all:
            result = [candidates[i:i + self.BATCH_SZ] for i in range(0, len(candidates), self.BATCH_SZ)]
        elif len(candidates) > self.BATCH_SZ:
            result = candidates[:self.BATCH_SZ]
        return result




class FollowNetPipe:



    async def run(self, tc: TwitchClient, streamer_pipe: StreamerPipe, q_out=None, n_consumers=50):
        q_foll_ids = asyncio.Queue()

        # Initialize producers and consumers for processing
        t_prod = asyncio.create_task(streamer_pipe.produce_follower_ids(tc, q_out=q_foll_ids), name='Followers')
        t_followings = [asyncio.create_task(
            self.produce_followed_ids(tc, q_in=q_foll_ids, q_out=q_out)) for _ in range(n_consumers)]
        # Block until producer and consumers are exhausted
        await asyncio.gather(t_prod)
        await q_foll_ids.join()
        # Cancel exhausted and idling consumers that are still waiting for items to appear in queue
        for t in t_followings:
            t.cancel()

        # Process any remaining batches
        remaining_batches = self.new_candidate_batches(remainder=True)
        if q_out:
            [q_out.put_nowait(batch) for batch in remaining_batches]


async def main():
    from pipes import Pipeline

    t = perf_counter()
    some_name = 'emilybarkiss'
    sample_sz = 350
    n_consumers = 100
    max_followings = 200


    async with TwitchClient() as tc:
        pipeline = Pipeline(tc)
        streamer_pipe = Streamer(some_name, sample_sz=sample_sz)
        folnet_pipe = FollowerNetwork(streamer_pipe, max_followings=max_followings, n_consumers=n_consumers)
        pipeline.add_pipe(streamer_pipe)
        pipeline.add_pipe(folnet_pipe)
        await pipeline.run(tc, streamer_pipe, n_consumers=n_consumers)

        streamer_pipe.display
        folnet_pipe.display

        print(f'{Col.magenta}[🟊] N consumers: {n_consumers} {Col.end}')
        print(f'{Col.green}[🟊] Max Followings: {max_followings} {Col.end}')
        print(f'{Col.orange}[📞] Total Calls to Twitch: {tc.http.count_success_resp} {Col.end}')
        print(f'{Col.cyan}[⏲] Total Time: {round(perf_counter() - t, 3)} sec {Col.end}')
        from datetime import datetime
        print(f'{Col.red}\t««« {datetime.now().strftime("%I:%M.%S %p")} »»» {Col.end}')


if __name__ == "__main__":
    asyncio.run(main())
