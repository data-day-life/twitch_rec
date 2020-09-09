import asyncio
from twitch_client import TwitchClient
from bot_detection import BotDetector
from colors import Col
from dataclasses import dataclass
from typing import List
import re


@dataclass
class Streamer:
    name:        str = None
    uid:         str = None
    prof_img:    str = None
    caster_type: str = None
    view_count:  int = None
    total_folls: int = -1
    valid:       bool = False


    @staticmethod
    def _validate_name(name: str) -> str:
        if not name:
            raise ValueError('Provided name was empty ("") or "None".')
        # Strip any whitespace before matching to help the user; "Bob Ross" becomes a valid search
        matched = re.match(r"^(?!_)[a-zA-Z0-9_]{4,25}$", re.sub(r"\s+", "", name))
        if not matched:
            raise ValueError('Valid names are between 4-25 alpha-numeric characters (as well as "_"), '
                             'but may not contain any spaces or begin with "_".')

        return matched.string


    async def validate(self, tc, some_name: str = None):
        name = Streamer._validate_name(some_name or self.name)
        return await tc.validate_name_remote(name)


    async def create(self, tc: TwitchClient, some_name: str = None):
        if not self.valid:
            found = await self.validate(tc, some_name)
            self.name = found.display_name
            self.uid = found.id
            self.prof_img = found.profile_image
            self.caster_type = found.broadcaster_type
            self.view_count = found.view_count
            self.valid = True

        return self


    @property
    def display(self, result=''):
        try:
            result += f'{Col.bold}{Col.yellow}<<<<< Streamer:  {self.name}{Col.end}\n'
            result += f'\t\t{Col.yellow}🡲 uid: {self.uid}{Col.end}\n'
            result += f'{Col.white}  * Total followers: {self.total_folls}{Col.end}\n'
        except Exception as err:
            result += err

        return print(result)



class StreamerPipe:
    sanitized_follower_ids: List[str] = []

    def __init__(self, streamer: Streamer, sample_sz=300):
        if streamer is None:
            raise AttributeError('Streamer object provided to StreamerPipe was "None".')
        self.streamer = streamer
        self.sample_sz = sample_sz
        self.sanitized_follower_ids = list()
        self.bd = BotDetector()


    @property
    def display(self, result=''):
        result += f'{Col.bold}{Col.yellow}<<<<< Pipe: Streamer,  N={self.sample_sz}{Col.end}\n'
        result += f'{Col.white}  * {str(self.bd)}{Col.end}\n'
        result += f'{Col.yellow} > Follower ID List (sz={len(self.sanitized_follower_ids)}):{Col.end}\n'
        result += f'  {self.sanitized_follower_ids}\n'

        return print(result)


    async def __call__(self, tc: TwitchClient, q_out: asyncio.Queue = None):
        await self.produce_follower_ids(tc, q_out)


    @staticmethod
    def put_queue(id_list, q_out: asyncio.Queue = None):
        if q_out:
            [q_out.put_nowait(foll_id) for foll_id in id_list]


    async def produce_follower_ids(self, tc: TwitchClient, q_out: asyncio.Queue = None):
        """
        For a valid uid, collect a list of sanitized_follower_ids while removing follower bots.  Batches of
        sanitized uids are placed into a given queue.

        Args:
            tc (TwitchClient):
                A twitch client; used to collect follower information for a uid.

            q_out (asyncio.Queue):
                The worker queue where follower ids are placed; fetches followings for the valid follower_id.

        Returns:
            A list of sanitized follower uids; length is not necessarily equal to sample_sz.
        """
        try:
            await self.streamer.create(tc)
        except Exception as err:
            raise AttributeError(f'{err} Unable to produce follower ids.')

        return await self.fetch_follower_ids(tc, q_out)


    async def fetch_follower_ids(self, tc: TwitchClient, q_out: asyncio.Queue = None):
        follower_reply = await tc.get_n_followers(self.streamer.uid, n_folls=self.sample_sz, full_reply=True)
        next_cursor = follower_reply.get('cursor')
        self.streamer.total_folls = follower_reply.get('total', 0)

        # Sanitize first fetch, then sanitize remaining fetches
        all_sanitized_uids = self.bd.sanitize_foll_list(follower_reply.get('data'))
        self.put_queue(all_sanitized_uids, q_out)

        while next_cursor and len(all_sanitized_uids) < self.sample_sz:
            params = [('after', next_cursor)]
            next_foll_reply = await tc.get_n_followers(self.streamer.uid, params=params, full_reply=True)
            next_cursor = next_foll_reply.get('cursor')
            next_sanitized_uids = self.bd.sanitize_foll_list(next_foll_reply.get('data'))
            all_sanitized_uids.extend(next_sanitized_uids)
            self.put_queue(next_sanitized_uids, q_out)

        self.sanitized_follower_ids = all_sanitized_uids
        return self.sanitized_follower_ids


async def main():
    from time import perf_counter
    t = perf_counter()

    some_name = 'emilybarkiss'
    sample_sz = 350

    async with TwitchClient() as tc:
        streamer = await Streamer(some_name).create(tc)
        str_pipe = StreamerPipe(streamer, sample_sz=sample_sz)
        await str_pipe(tc)

    streamer.display
    str_pipe.display
    print(f'{Col.orange}[📞] Total Calls to Twitch: {tc.http.count_success_resp} {Col.end}')
    print(f'{Col.cyan}[⏲] Total Time: {round(perf_counter() - t, 3)} sec {Col.end}')


if __name__ == "__main__":
    asyncio.run(main())
