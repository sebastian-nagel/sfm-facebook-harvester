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
import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait


from sfmutils.harvester import BaseHarvester, Msg, CODE_TOKEN_NOT_FOUND, CODE_UID_NOT_FOUND, CODE_UNKNOWN_ERROR
from sfmutils.warcprox import warced
from sfmutils.utils import safe_string



log = logging.getLogger(__name__)



QUEUE = "facebook_rest_harvester"
TIMELINE_ROUTING_KEY = "harvest.start.facebook.facebook_user_timeline"
BIO_ROUTING_KEY = "harvest.start.facebook.facebook_user_bio"
ADS_ROUTING_KEY = "harvest.start.facebook.facebook_user_ads"

base_fb_url = "https://www.facebook.com/"

class FacebookHarvester(BaseHarvester):

    def __init__(self, working_path, stream_restart_interval_secs=30 * 60, mq_config=None,debug=False,
                 connection_errors=5, http_errors=5, debug_warcprox=False, tries=3):
        BaseHarvester.__init__(self, working_path, mq_config=mq_config, use_warcprox = True,
                               stream_restart_interval_secs=stream_restart_interval_secs,
                               debug=debug, debug_warcprox=debug_warcprox, tries=tries)

        self.connection_errors = connection_errors
        self.http_errors = http_errors
        # pages attribute for facebookscarper - how far 'back' should the scraper look?
        self.pages = 1000 # this is the number of pages that facebook_scraper will scrape - could later be adapted
        self.harvest_media_types = { 'photo': True }


    def initiate_selenium_webdriver(self):
        """
        Instantiates selenium webdriver with docker connection for sfm
        Closing should take place outside of this function!
        """
        user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36'

        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('headless')
        chrome_options.add_argument('start-maximised')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--window-size=1200x800')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument(f"user-agent={user_agent}")

        # this will connect to the selenium container starting scraping
        # Note: host name of the running container is "selenium"
        driver = webdriver.Remote("http://selenium:4444/wd/hub", {'browserName': 'chrome'})
        return driver

    def fb_login(self, driver):
        """
        Logs into fb via selenium via a preexisting selenium session
        """

        user_email_fb = self.message['credentials']['user_email_fb']
        user_password_fb = self.message['credentials']['user_password_fb']


        driver.get("http://m.facebook.com")
        driver.maximize_window()
        # accept cookies
        cookies = driver.find_element_by_id('accept-cookie-banner-label')
        # more or less random wait to replicate user behavior, ensure politeness
        time.sleep(random.uniform(3,9))
        cookies.click()
        # Search & Enter the Email or Phone field & Enter Password
        username_fb = driver.find_element_by_id("m_login_email")
        password_fb = driver.find_element_by_id("m_login_password")
        submit  = driver.find_element_by_css_selector("._56b_")
        # send keys and make sure not prepolutaed
        # 2fa has to be deactivated
        username_fb.clear()
        password_fb.clear()
        username_fb.send_keys(user_email_fb)
        password_fb.send_keys(user_password_fb)
        time.sleep(random.uniform(3,9))
        # Step 4) Click Login
        submit.click()





    def get_fbid(self, username):
        """
        Attempts to scrape fb id from fb pages. Username should be full
        FB Link, if not this will construct it from the username.
        """
        headers = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"}


        if username.startswith("https://www.facebook.com/") == False and username.startswith("http://www.facebook.com/") == False:

            # catch weird fb urls (e.g. https://es-es.facebook.com/ehbildu/)
            # as of 2020/12 deactivated to ensure continuity between provided seeds
            # and harvested seeds
            # username = re.sub(r'^.+facebook\.[a-z]+/', '', username)

            # possibly add www.facebook.com
            username = base_fb_url + str(username)

        driver = self.initiate_selenium_webdriver()
        self.fb_login(driver = driver)

        driver.get(username)

        # r = requests.get(username, headers = headers)
        # parse with bs4 - we could also do this with
        # selenium but this enables us to reuse 'old' code instead of rewriting
        soup = BeautifulSoup(driver.page_source, "html.parser")
        driver.quit()
        # getting id, still a little crude, todo
        id = soup.find('meta', {"property" : "al:android:url"})
        if not id:
            log.error("Facebook ID not found in request to site: %s", username)
            log.info("Metadata elements found: %s", soup.find_all('meta'))
            raise ValueError("Facebook ID not found")

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
        being harvested but this could change
        """

        harvest_type = self.message.get("type")
        # Dispatch message based on type

        log.debug("Harvest type is %s", harvest_type)


        if harvest_type == "facebook_user_timeline":
            log.debug("Starting Facebook timeline harvest")
            self.facebook_users_timeline()
        elif harvest_type == "facebook_user_bio":
            self.facebook_users_bio()
            log.debug("Starting Facebook bio harvest")
        elif harvest_type == "facebook_user_ads":
            log.debug("Starting Facebook ads harvest")
            self.facebook_users_ads()
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

        # possibly get fbid from state.json
        if not nsid:
            nsid = self.state_store.get_state(__name__, u"timeline.{}.fbid".format(username))
            log.info("Trying to retrieve FB-ID from state store")
            log.info("Found FB-ID from state store is %s", nsid)


        # Possibly look up fbid if not supplied and not already in state.json
        if username and not nsid:

            log.debug("No FB userid, retrieving it")

            nsid = self.get_fbid(username)
            # write id to state.json if not already there
            key = "timeline.{}.fbid".format(username)
            self.state_store.set_state(__name__, key, nsid)
            log.info("Writing fbid to state store")

        if nsid:
            # report back whether user id was found
            log.info("FB userid %s", nsid)
            # todo - need to add timeout and what to do if blocked

            incremental = self.message.get("options", {}).get("incremental", False)
            harvest_media = self.message.get("options", {}).get("harvest_media", False)
            if harvest_media:
                # disable verbose logging of the state store to avoid overlong log messages
                # (the state store holds the list of all harvested media URLs)
                self.state_store.verbose = False

            if incremental:
                # search for since_id of post
                since_id = self.state_store.get_state(__name__, u"timeline.{}.since_id".format(nsid))

            scrape_result = []

            for post in facebook_scraper.get_posts(nsid, pages = self.pages, extra_info = True, timeout = 20):
                scrape_result.append(post)
                self.result.harvest_counter["posts"] += 1
                self.result.increment_stats("posts")

                if harvest_media and post['images']: #last condition avoids parsing empty lists (i.e. no media)
                    log.info("Harvesting media from post")
                    # get media content from links - should automatically be caught within warc stream
                    # all photos on fb are jpgs, so the list comprehension checks whether this is the case
                    # for the stream, if not (e.g. video) it will not harvest
                    [self._harvest_media_url(media_url) for media_url in post['images'] if 'jpg' in media_url]

                if incremental and post["post_id"] == since_id and post["post_id"]:
                    log.info("Stopping, found last post that was previously harvested with id: %s", post["post_id"])
                    break

            # filename will later be converted to path
            # replicating pattern from https://github.com/internetarchive/warcprox/blob/f19ead00587633fe7e6ba6e3292456669755daaf/warcprox/writer.py#L69
            # create random token for filename
            random_token = ''.join(random.sample('abcdefghijklmnopqrstuvwxyz0123456789', 8))
            serial_no = '00000'
            file_name = safe_string(self.message["id"]) + "-" + warcprox.timestamp17() + "-" + serial_no + "-" + random_token

            with open(os.path.join(self.warc_temp_dir, file_name + ".warc.gz"), "wb") as result_warc_file:
                log.info("Writing json-timeline result to path %s", self.warc_temp_dir)
                writer = WARCWriter(result_warc_file, gzip = True)

                def json_date_converter(o):
                    """ Converts datetime.datetime items in facebook_scraper result
                    to formate suitable for json.dumps"""
                    if isinstance(o, datetime.datetime):
                        return o.__str__()

                json_payload = json.dumps(scrape_result, default = json_date_converter,
                                          ensure_ascii = False).encode("utf-8")


                record = writer.create_warc_record(username, 'metadata',
                                                    payload = BytesIO(json_payload),
                                                    warc_content_type = "application/json")
                writer.write_record(record)
                log.info("Writing scraped results to %s", self.warc_temp_dir)

            # write most recent post ID to state store
            key = "timeline.{}.since_id".format(nsid)
            max_post_time = scrape_result[0].get("time")
            max_post_id = scrape_result[0].get("post_id")

            assert max_post_time and max_post_id

            if incremental:
                self.state_store.set_state(__name__, key, max_post_id)
                log.info("Wrote first scraped post to state_store: %s (state: %s)",
                         max_post_id, key)


        else:
            msg = "NSID not found for user {}".format(username)
            log.exception(msg)
            self.result.warnings.append(Msg(CODE_UID_NOT_FOUND, msg, seed_id=seed_id))
        # todo: deal with blocking (i.e.: wait 24 hours until resuming harvest)

    def _harvest_media_url(self, url):

        media_urls = self.state_store.get_state(__name__, 'media.urls')
        if media_urls is None:
            media_urls = dict()
        if url in media_urls:
            log.info("Media URL %s already harvested at %s", url, media_urls[url])
            return

        try:
            r = requests.get(url)
            log.info("Harvested media URL %s (status: %i, content-type: %s)",
                     url, r.status_code, r.headers['content-type'])
            media_urls[url] = str(datetime.datetime.fromtimestamp(time.time()))
            self.state_store.set_state(__name__, 'media.urls', media_urls)

            time.sleep(2.5) # must sleep to ensure politeness and avoid blocking

        except Exception:
            log.exception("Failed to harvest media URL %s with exception:", url)

    def facebook_users_bio(self):

        for seed in self.message.get("seeds", []):

            username = seed.get("token")

            # check whether it already has been scraped, in that case do not scrape bio
            prev_harvest = self.state_store.get_state(__name__, "bio.{}".format(username))

            if prev_harvest is None:
                # harvest
                self.facebook_user_bio(username = username)

                # write to state store
                key = "bio.{}".format(username)
                self.state_store.set_state(__name__, key, True)
                # for a large number of sites we avoid to many requests
                # also adding random  float number between 0 and 1 todo via random.random
                time.sleep((5))
            elif prev_harvest:
                log.info("Bio of this account has already been harvested - stopping")


    def facebook_user_bio(self, username):
        """Scrapes Facebook bio and returns info
        on the information contained on the about page (e.g. https://www.facebook.com/pg/SPD/about/?ref=page_internal)
        @param username: Facebook username
        @return: a dictionary of account attributes """


        # ensure username is clean and can be accessed
        if username.startswith("https://www.facebook.com/") or username.startswith("http://www.facebook.com/") \
        or username.startswith("www.facebook.com/"):

            username = re.sub(r'^.+facebook\.com\/', '', username)
            # possibly also remove trailing /
            username = re.sub(r'\/$', '', username)

        # created at field
        fb_general = base_fb_url + username
        # bio info
        fb_about = base_fb_url +  username + "/about/?ref=page_internal"
        # site transparency (e.g. admins)
        m_fb_general = "https://m.facebook.com/" + username

        # request the html
        # r = requests.get(fb_general)
        # # ensure no 404's
        # if not r:
        #     log.debug("Couldn't access profile site: %s", fb_general)
        #     return
        #
        # soup = BeautifulSoup(r.content, "html.parser")

        # scrape creation date
        # created_at = soup.find('div', {"class" : "s9t1a10h"})
        # created_at = created_at.select_one("span").text
        # could be used to extract exact date without text
        # however, this may differ across languages and pages
        # created_at = re.sub(r"(Seite erstellt)", "", created_at)
        # created_at = created_at[3:]

        # scrape n of likes
        # find span with like number
        # spans = soup.find('span', {"class" : "_52id _50f5 _50f7"})
        # # isolate likes via regex
        # likes = re.search(r'^[\d]+.[^\s]+', spans.text).group()
        site_transparency_detail_id_selector = "div[id^='u_0_d']"
        site_transparency_class_selector = "._a58._a5o._9_7._2rgt._1j-g._2rgt._86-3._2rgt._1j-g._2rgt"

        driver = self.initiate_selenium_webdriver()

        self.fb_login(driver = driver)
        time.sleep(random.uniform(3,9))
        # navigate to site
        driver.get(m_fb_general)
        time.sleep(random.uniform(3,9))
        # site info only loads on scroll
        driver.execute_script("window.scrollTo(0, 800)")

        # extract likes
        site_likes_xpath = "//div[@class=\"_59k _2rgt _1j-f _2rgt\"]"
        likes = driver.find_elements_by_xpath(site_likes_xpath)
        # extract the relevant div
        likes = [single_div.text for single_div in likes if "like this" in single_div.text]

        # use class name and div content (todo)
        time.sleep(random.uniform(20, 25))
        element = WebDriverWait(driver, 20).until(
                ec.presence_of_element_located((By.CSS_SELECTOR, site_transparency_class_selector))
            )
        site_transparency = driver.find_elements_by_css_selector(site_transparency_class_selector)
        # site transparency should always be below about
        time.sleep(random.uniform(5, 9))
        site_transparency[1].click()
        time.sleep(random.uniform(15, 20))
        # simply get the whole text of the transparency box of site
        # the exact info can be extracted ex-post
        element = WebDriverWait(driver, 20).until(
                ec.presence_of_element_located((By.CSS_SELECTOR, site_transparency_detail_id_selector))
            )
        time.sleep(random.uniform(3,9))
        site_transparency_text = driver.find_element_by_css_selector(site_transparency_detail_id_selector).text
        time.sleep(random.uniform(3,9))
        driver.quit()
        log.info("Finished scraping transparency box")
        bio_dict = {"username" : fb_general,
                    "n_likes" : likes[0],
                    "transparency_text" : site_transparency_text}

        # request about html
        r_about = requests.get(fb_about)

        # ensure no 404's
        if not r_about:
            log.debug("Couldn't access username/about site: %s", fb_about)
            return


        about_soup = BeautifulSoup(r_about.content, "html.parser")
        mission_text = about_soup.find_all('div', {'class' : "_4bl9"})

        for divs in mission_text:
            describing_div = divs.find('div', {'class': '_50f4'})
            content_div = divs.find('div', {'class': '_3-8w'})

            if describing_div and content_div:
                bio_dict[describing_div.text] = content_div.text

        # photos
        # Retrieves profile and cover photo of public facebook page
        # bio going to the 'about' page, parsing html and getting
        # the links to photos from script tag, these can then be passed
        # harvest_media
        # this is not affected by the harvest_media options but will always happen
        all_scripts = about_soup.find_all('script')

        for js in all_scripts:
            for content in js.contents:
                if 'cover_photo' in content:
                    # isolate relevant links
                    links = re.findall(r'https\:\\/\\/scontent[^"]*', content)

                    # remove escaped front slashes
                    for val, link in enumerate(links):
                        links[val] = re.sub(r'\\', "", link)
                        self._harvest_media_url(links[val])
                # ensure that only warc will be written if sites were found
                # else nothing will happen
        if r_about or r:
            # filename will later be converted to path
            # replicating pattern from https://github.com/internetarchive/warcprox/blob/f19ead00587633fe7e6ba6e3292456669755daaf/warcprox/writer.py#L69
            # create random token for filename
            random_token = ''.join(random.sample('abcdefghijklmnopqrstuvwxyz0123456789', 8))
            serial_no = '00000'
            file_name = safe_string(self.message["id"]) + "-" + warcprox.timestamp17() + "-" + serial_no + "-" + random_token

            with open(os.path.join(self.warc_temp_dir, file_name + ".warc.gz"), "wb") as result_warc_file:
                log.info("Writing json-timeline result to path %s", self.warc_temp_dir)
                writer = WARCWriter(result_warc_file, gzip = True)

                def json_date_converter(o):
                    """ Converts datetime.datetime items in facebook_scraper result
                    to formate suitable for json.dumps"""
                    if isinstance(o, datetime.datetime):
                        return o.__str__()

                json_payload = json.dumps(bio_dict, default = json_date_converter,
                                          ensure_ascii = False).encode("utf-8")


                record = writer.create_warc_record("https://m.facebook.com/" + username, 'metadata',
                                                    payload = BytesIO(json_payload),
                                                    warc_content_type = "application/json")
                writer.write_record(record)
                log.info("Writing scraped results to %s", self.warc_temp_dir)



    def facebook_users_ads(self):
        """Get multiple profile ads from api ads library """

        access_token = self.message["credentials"]["access_token_fb"] if self.message.get("credentials", False) else None


        # ads library api needs the iso2c code
        # this should be directly supplied with the message and come from the
        # harvest message
        for seed in self.message.get("seeds", []):

            # retrieve nested account info
            account_info = seed.get("token")

            username = seed.get("username")
            nsid = seed.get("uid")
            iso2c = seed.get("iso2c")

            # pass to actual harvester that will make api calls
            self.facebook_user_ads(username = username, nsid = nsid, iso2c = iso2c, access_token = access_token)



if __name__ == "__main__":
    FacebookHarvester.main(FacebookHarvester, QUEUE, [TIMELINE_ROUTING_KEY, BIO_ROUTING_KEY, ADS_ROUTING_KEY])
