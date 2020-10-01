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
import warcprox
import random

from sfmutils.harvester import BaseHarvester, Msg, CODE_TOKEN_NOT_FOUND, CODE_UID_NOT_FOUND, CODE_UNKNOWN_ERROR
from sfmutils.warcprox import warced
from sfmutils.utils import safe_string

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

                incremental = self.message.get("options", {}).get("incremental", False)

                if incremental:

                    self.state_store.set_state(__name__, u"{}.since_id".format(self._search_id()))



            def json_date_converter(o):
                """ Converts datetime.datetime items in facebook_scraper result
                to formate suitable for json.dumps"""
                if isinstance(o, datetime.datetime):
                    return o.__str__()

            # filename will later be converted to path
            # replicating pattern from https://github.com/internetarchive/warcprox/blob/f19ead00587633fe7e6ba6e3292456669755daaf/warcprox/writer.py#L69
            # create random token for filename
            random_token = ''.join(random.sample('abcdefghijklmnopqrstuvwxyz0123456789', 8))
            serial_no = '00000'
            file_name = safe_string(self.message["id"]) + "-" + warcprox.timestamp17() + "-" + serial_no + "-" + random_token

            with open(os.path.join(self.warc_temp_dir, file_name + ".warc.gz"), "wb") as result_warc_file:
                log.info("Writing json-timeline result to path", str(self.warc_temp_dir))
                writer = WARCWriter(result_warc_file, gzip = True)

                json_payload = json.dumps(scrape_result, default = json_date_converter).encode("utf-8")


                record = writer.create_warc_record("https://m.facebook.com/" + username, 'metadata',
                                                    payload = BytesIO(json_payload),
                                                    warc_content_type = "application/json")
                writer.write_record(record)
                log.info("Writing scraped results to %s", self.warc_temp_dir)
        else:
            msg = "NSID not found for user {}".format(username)
            log.exception(msg)
            self.result.warnings.append(Msg(CODE_UID_NOT_FOUND, msg, seed_id=seed_id))
        # todo: deal with blocking (i.e.: wait 24 hours until resuming harvest)

    def _search_id(self):

        query = todo

        return query

    def process_search_warc(self, warc_filepath):

        incremental = self.message.get("options", {}).get("incremental", False)

        since_id = self.state_store.get_state(__name__, u"{}.since_id".format(self._search_id())) if incremental else None


if __name__ == "__main__":
    FacebookHarvester.main(FacebookHarvester, QUEUE, [TIMELINE_ROUTING_KEY])
