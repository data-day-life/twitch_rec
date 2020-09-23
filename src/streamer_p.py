import asyncio
from twitch_client import TwitchClient
from bot_detection import BotDetector
from colors import Col
from pipes import Pipe
from typing import List
import re


class Streamer(Pipe):
    name:        str = None
    uid:         str = None
    prof_img:    str = None
    caster_type: str = None
    view_count:  int = None
    total_folls: int = -1
    valid:       bool = False
    sanitized_follower_ids: List[str] = []


    def __init__(self, given_name, sample_sz=300):
        self.name = Streamer.validate_name(given_name)
        self.sample_sz = sample_sz
        self.bd = BotDetector()
        pass


    async def __call__(self, tc: TwitchClient):
        await self.produce(tc)
        return self


    @staticmethod
    def validate_name(name: str) -> str:
        if not name:
            raise ValueError('Provided name was empty ("") or "None".')
        # Strip any whitespace before matching to help the user; "Bob Ross" becomes a valid search
        matched = re.match(r"^(?!_)[a-zA-Z0-9_]{4,25}$", re.sub(r"\s+", "", name))
        if not matched:
            raise ValueError('Valid names are between 4-25 alpha-numeric characters (as well as "_"), '
                             'but may not contain any spaces or begin with "_".')
        return matched.string


    async def create(self, tc: TwitchClient):
        if not self.valid:
            found = await tc.validate_name_remote(self.name)
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
            result += f'{Col.bold}{Col.yellow}>>>>> Pipe || Streamer  ' \
                      f'(N={self.sample_sz}) {Col.end}\n'
            result += f'{Col.yellow} 🡲 {self.name}  (uid: {self.uid}) {Col.end}\n'
            result += f'{Col.white}   * Total followers: {self.total_folls}{Col.end}\n'
            result += f'{Col.white}   * {str(self.bd)}{Col.end}\n'
            result += f'{Col.yellow} -> Follower ID List (sz={len(self.sanitized_follower_ids)}):{Col.end}\n'
            result += f'    {self.sanitized_follower_ids}\n'
        except Exception as err:
            result += err
        return print(result)


    async def create_tasks(self, tc: TwitchClient):
        self.tasks.append(asyncio.create_task(self.produce(tc)))


    async def produce(self, tc):
        try:
            await self.create(tc)
        except Exception as err:
            raise AttributeError(f'{err} Unable to produce follower ids.')
        return await self.fetch_follower_ids(tc)


    async def fetch_follower_ids(self, tc: TwitchClient):
        follower_reply = await tc.get_n_followers(self.uid, n_folls=self.sample_sz, full_reply=True)
        next_cursor = follower_reply.get('cursor')
        self.total_folls = follower_reply.get('total', 0)

        # Sanitize first fetch, then sanitize remaining fetches
        all_sanitized_uids = self.bd.sanitize_foll_list(follower_reply.get('data'))
        self.put_queue(all_sanitized_uids)

        while next_cursor and len(all_sanitized_uids) < self.sample_sz:
            params = [('after', next_cursor)]
            next_foll_reply = await tc.get_n_followers(self.uid, params=params, full_reply=True)
            next_cursor = next_foll_reply.get('cursor')
            next_sanitized_uids = self.bd.sanitize_foll_list(next_foll_reply.get('data'))
            if next_sanitized_uids:
                self.put_queue(next_sanitized_uids)
                all_sanitized_uids.extend(next_sanitized_uids)

        self.sanitized_follower_ids = all_sanitized_uids
        return self.sanitized_follower_ids


    def put_queue(self, id_list):
        if self._q_out:
            [self._q_out.put_nowait(foll_id) for foll_id in id_list]


async def main():
    from time import perf_counter
    t = perf_counter()

    some_name = 'emilybarkiss'
    sample_sz = 350

    async with TwitchClient() as tc:
        streamer = Streamer(some_name, sample_sz)
        await streamer(tc)

    streamer.display
    print(f'{Col.orange}[📞] Total Calls to Twitch: {tc.http.count_success_resp} {Col.end}')
    print(f'{Col.cyan}[⏲] Total Time: {round(perf_counter() - t, 3)} sec {Col.end}')


if __name__ == "__main__":
    asyncio.run(main())
