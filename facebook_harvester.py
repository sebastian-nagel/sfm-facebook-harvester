# facebook harvester
import facebook_scraper
import logging
import re
import json

from sfmutils.harvester import BaseHarvester, Msg, CODE_TOKEN_NOT_FOUND, CODE_UID_NOT_FOUND, CODE_UNKNOWN_ERROR

log = logging.getLogger(__name__)

QUEUE = "facebook_rest_harvester"
TIMELINE_ROUTING_KEY = "harvest.start.facebook.facebook_user_timeline"

class FacebookHarvester(BaseHarvester):

    def __init__(self, working_path, stream_restart_interval_secs=30 * 60, mq_config=None,debug=False,
                 connection_errors=5, http_errors=5, debug_warcprox=False, tries=3):
        BaseHarvester.__init__(self, working_path, mq_config=mq_config, use_warcprox = False,
                               stream_restart_interval_secs=stream_restart_interval_secs,
                               debug=debug, debug_warcprox=debug_warcprox, tries=tries)

        self.connection_errors = connection_errors
        self.http_errors = http_errors

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
            pass
            if nsid:
                # report back whether user id was found
                facebook_scraper.get_posts(nsid, pages = 1, extra_info = True, timeout = 20)
 # todo

            else:
                msg = "NSID not found for user {}".format(username)
                log.exception(msg)
                self.result.warnings.append(Msg(CODE_UID_NOT_FOUND, msg, seed_id=seed_id))

        # now start scraping with facebook scraper
        facebook_scraper.get_posts(nsid, pages = 1, extra_info = True, timeout = 20)

        result = list(facebook_scraper.get_posts(nsid, pages = 1, extra_info = True, timeout = 20))
        print(result[0])



if __name__ == "__main__":
    FacebookHarvester.main(FacebookHarvester, QUEUE, [TIMELINE_ROUTING_KEY])
