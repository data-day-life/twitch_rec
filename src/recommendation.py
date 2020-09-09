import asyncio
from datetime import datetime as datetime
from time import perf_counter
from twitch_client import TwitchClient
from streamer import Streamer
from follower_network import FollowerNetwork
from live_stream_info import LiveStreams
from recommendation_pipeline import RecommendationPipeline
from similarity import JaccardSim
from collections import OrderedDict
from colors import Col


class Recommendation:
    sample_sz:      int = 350
    max_followings: int = 150
    min_mutual:     int = 3
    pipeline:       RecommendationPipeline
    similarities:   JaccardSim

    def __init__(self, streamer_name: str, sample_sz=300, max_followings=200, min_mutual=3) -> None:
        self.sample_sz = sample_sz
        self.max_followings = max_followings
        self.min_mutual = min_mutual

        self.streamer = Streamer(name=streamer_name)
        self.folnet = FollowerNetwork(streamer_id=self.streamer.uid, min_mutual=self.min_mutual)
        self.live_streams = LiveStreams()

        self.pipeline = RecommendationPipeline(self.streamer, self.folnet, self.live_streams,
                                               max_followings=self.max_followings, sample_sz=self.sample_sz)



    def get_sims(self):
        results = OrderedDict()
        mutual_followings, tot_followers = self.folnet.mutual_followings, self.live_streams.total_followers
        num_collected = self.pipeline.folnet_pipe.num_collected
        ranked_sims = JaccardSim(mutual_followings, tot_followers, num_collected).ranked_sim_scores

        for uid in ranked_sims:
            got = self.live_streams.get(uid)
            formatted = f'\n' \
                        f'{got["user_name"]:>18}  ' \
                        f'{got["viewer_count"]:>4}  ' \
                        f'{got["stream_duration"]:>10}   ' \
                        f'{got["language"]:>2}  ' \
                        f'{got["total_followers"]:>4}  ' \
                        f'{ranked_sims.get(uid) * 100:.3f}  ' \
                        f'{mutual_followings.get(uid):>3}'

            print(formatted)
            results.update({uid: self.live_streams.get(uid)})

        return results


    async def __call__(self, n_consumers=100):
        t = perf_counter()


        async with TwitchClient() as tc:
            await self.streamer.create(tc)
            # self.pipeline = RecommendationPipeline(self.streamer, self.folnet, self.live_streams)
            await self.pipeline(tc, n_consumers)

            self.streamer.display
            self.pipeline.streamer_pipe.display
            self.pipeline.folnet_pipe.display
            self.pipeline.live_stream_pipe.display

            self.get_sims()

            print(f'{Col.magenta}[🟊] N consumers: {n_consumers} {Col.end}')
            print(f'{Col.green}[🟊] Max Followings: {self.max_followings} {Col.end}')
            print(f'{Col.orange}[📞] Total Calls to Twitch: {tc.http.count_success_resp} {Col.end}')
            print(f'{Col.white}\t(Token bucket: {tc.http._bucket.tokens}{Col.end})')
            print(f'{Col.cyan}[⏲] Total Time: {round(perf_counter() - t, 3)} sec {Col.end}')
            print(f'{Col.red}\t««« {datetime.now().strftime("%I:%M.%S %p")} »»» {Col.end}')



    def displ_fmt(self, name, viewers, duration, lang, total_folls, sim_score, mutual_count):
        pass


# live_list = {
#             usr['user_id']: {
#                      'name': usr['user_name'],
#                      'stream_title': usr['title'],
#                      'stream_url': 'https://www.twitch.tv/' + usr['user_name'],
#                      'thumbnail_url': usr['thumbnail_url'],
#                      'viewer_count': usr['viewer_count'],
#                      'stream_duration': duration(usr['started_at']),
#                      'lang': usr['language']
#                     }
#             for usr in live_list}


async def main():
    name = 'funfps'
    sample_sz = 400
    max_followings = 200
    min_mutual = 3

    rec = Recommendation(name, sample_sz, max_followings, min_mutual)
    await rec()

if __name__ == "__main__":
    asyncio.run(main())
