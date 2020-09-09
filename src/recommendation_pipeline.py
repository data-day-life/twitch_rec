import asyncio
from twitch_client import TwitchClient
from streamer import StreamerPipe, Streamer
from follower_network import FollowNetPipe, FollowerNetwork
from live_stream_info import LiveStreamPipe, LiveStreams


class RecommendationPipeline:

    # TODO: want this to take instantiated objects as params instead of arguments to instantiate the objects
    def __init__(self, streamer: Streamer, folnet: FollowerNetwork, live_streams: LiveStreams,
                 max_followings: int = 150, sample_sz: int = 300) -> None:
        self.streamer_pipe = StreamerPipe(streamer, sample_sz=sample_sz)
        self.folnet_pipe = FollowNetPipe(folnet, max_followings=max_followings)
        self.live_stream_pipe = LiveStreamPipe(live_streams)


    async def __call__(self, tc: TwitchClient, n_consumers: int):
        q_foll_ids = asyncio.Queue()
        q_followings = asyncio.Queue()
        q_live_uids = asyncio.Queue()

        await self.streamer_pipe(tc, q_out=q_foll_ids)

        # t_prod = asyncio.create_task(self.streamer_pipe(tc, q_out=q_foll_ids))
        t_followings = [asyncio.create_task(
            self.folnet_pipe.produce_followed_ids(tc, q_in=q_foll_ids, q_out=q_followings)) for _ in range(n_consumers)]
        t_livestreams = asyncio.create_task(
            self.live_stream_pipe.produce_live_streams(tc, q_in=q_followings, q_out=q_live_uids))
        t_total = [asyncio.create_task(
            self.live_stream_pipe.consume_live_streams(tc, q_in=q_live_uids)) for _ in range(n_consumers // 2)]

        # Streamer: follower ids
        # await asyncio.gather(t_prod)

        # Folnet: follower's followings
        await q_foll_ids.join()
        [q_followings.put_nowait(batch) for batch in self.folnet_pipe.new_candidate_batches(remainder=True)]
        [t.cancel() for t in t_followings]

        # LiveStreams
        await q_followings.join()
        t_livestreams.cancel()

        await q_live_uids.join()
        [t.cancel() for t in t_total]


async def main():
    from colors import Col
    from datetime import datetime
    from time import perf_counter
    t = perf_counter()

    some_name = 'emilybarkiss'
    sample_sz = 350
    n_consumers = 100

    async with TwitchClient() as tc:
        streamer = Streamer(name=some_name)
        folnet = FollowerNetwork(streamer.uid)
        livestreams = LiveStreams()

        pipeline = RecommendationPipeline(streamer, folnet, livestreams, sample_sz)
        await pipeline(tc, n_consumers)

    streamer.display
    pipeline.folnet_pipe.display
    pipeline.live_stream_pipe.display

    print(f'{Col.magenta}🟊 N consumers: {n_consumers} {Col.end}')
    print(f'{Col.cyan}⏲ Total Time: {round(perf_counter() - t, 3)} sec {Col.end}')
    print(f'{Col.red}\t««« {datetime.now().strftime("%I:%M.%S %p")} »»» {Col.end}')


if __name__ == "__main__":
    asyncio.run(main())
