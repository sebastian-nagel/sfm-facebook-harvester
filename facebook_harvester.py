# facebook harvester
import facebook_scraper
import logging
import re
import json
from bs4 import BeautifulSoup
from warcio.warcwriter import WARCWriter
import requests
import os
import datetime
from io import BytesIO


from sfmutils.harvester import BaseHarvester, Msg, CODE_TOKEN_NOT_FOUND, CODE_UID_NOT_FOUND, CODE_UNKNOWN_ERROR
from  sfmutils.warcprox import warced

log = logging.getLogger(__name__)

QUEUE = "facebook_rest_harvester"
TIMELINE_ROUTING_KEY = "harvest.start.facebook.facebook_user_timeline"

class FacebookHarvester(BaseHarvester):

    def __init__(self, working_path, stream_restart_interval_secs=30 * 60, mq_config=None,debug=False,
                 connection_errors=5, http_errors=5, debug_warcprox=False, tries=3):
        BaseHarvester.__init__(self, working_path, mq_config=mq_config, use_warcprox = True,
                               stream_restart_interval_secs=stream_restart_interval_secs,
                               debug=debug, debug_warcprox=debug_warcprox, tries=tries)

        self.connection_errors = connection_errors
        self.http_errors = http_errors
        # pages attribute for facebookscarper - how far 'back' should the scraper look?
        self.pages = 1000

#
# python facebook_harvester.py seed test.json . --tries 1

    def get_fbid(self, username):
        """
        Attempts to scrape fb id from fb pages. Username should be full
        FB Link, if not this will construct it from the username.
        """


        base_fb_url = "https://www.facebook.com/"

        if username.startswith("https://www.facebook.com/") == False and username.startswith("http://www.facebook.com/") == False:
            username = base_fb_url + str(username)


        r = requests.get(username)

        soup = BeautifulSoup(r.content, "html.parser")

        # getting id, still a little crude, todo
        id = soup.find('meta', {"property" : "al:android:url"})

        id = id.get('content')

        if id.endswith('?referrer=app_link'):
            id = id[:-18]

        if id.startswith('fb://page/'):
            id = id[10:]

        return(id)



    def harvest_seeds(self):
        """
        Will start appropriate harvest - as of now this
        is unnecessary as timelines are the only fb types
        bein harvested but this could change
        """

        # Dispatch message based on type
        harvest_type = "Facebook Timeline Harvest"
        log.debug("Harvest type is %s", harvest_type)


        if harvest_type == "Facebook Timeline Harvest":
            log.debug("Starting timeline harvest")
            self.facebook_users_timeline()

        else:

            raise KeyError



    def facebook_users_timeline(self):
        """Several users"""

        log.debug("Harvesting users with seeds %s", self.message.get("seeds"))

        for seed in self.message.get("seeds", []):   #todo
            self.facebook_user_timeline(seed_id = seed.get("id"), username = seed.get("token"), nsid = seed.get("uid"))


    def facebook_user_timeline(self, seed_id, username, nsid):
        """This function will scrape the user timeline"""
        log.debug("Harvesting user %s with seed_id %s.", username, seed_id)
        # make sure either username or nsid is present to start scraping
        assert username or nsid

        # Possibly look up username
        if username and not nsid:
            #todo lookup username
            log.debug("No FB userid, retrieving it")

            nsid = self.get_fbid(username)


        if nsid:
            # report back whether user id was found
            log.info("FB userid %s", nsid)
            # todo - need to add timeout and what to do if blocked

            scrape_result = []

            for post in facebook_scraper.get_posts(nsid, pages = 1, extra_info = True, timeout = 20):
                scrape_result.append(post)


            def json_date_converter(o):
                if isinstance(o, datetime.datetime):
                    return o.__str__()


            # todo, write this in same directionary as 'streamed' warc files
            if not os.path.exists('data'):
                os.makedirs('data')


            #with open(os.path.join(self.warc_temp_dir, str(nsid) + "warc.gz"), "wb") as result_warc_file:
            with open(os.path.join("data", str(nsid)) + "warc.gz", "wb") as result_warc_file:
                log.info("Writing json-timeline result to path", str(self.warc_temp_dir))
                writer = WARCWriter(result_warc_file, gzip = True)

                json_payload = json.dumps(scrape_result, default = json_date_converter).encode("utf-8")


                record = writer.create_warc_record(username, 'metadata',
                                                    payload = BytesIO(json_payload),
                                                    warc_content_type = "application/json")
                writer.write_record(record)
                log.info("Writing scraped results to %s", self.warc_temp_dir)


        else:
            msg = "NSID not found for user {}".format(username)
            log.exception(msg)
            self.result.warnings.append(Msg(CODE_UID_NOT_FOUND, msg, seed_id=seed_id))
        # todo: deal with blocking (i.e.: wait 24 hours until resuming harvest)





if __name__ == "__main__":
    FacebookHarvester.main(FacebookHarvester, QUEUE, [TIMELINE_ROUTING_KEY])
