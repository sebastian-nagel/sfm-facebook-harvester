#!/usr/bin/env python3

from __future__ import absolute_import
from sfmutils.warc_iter import BaseWarcIter
from dateutil.parser import parse as date_parse
import json
import sys

SEARCH_URL = "https://facebook.com/"

class FacebookWarcIter(BaseWarcIter):
    def __init__(self, filepaths, limit_user_ids=None):
        BaseWarcIter.__init__(self, filepaths)
        self.limit_user_ids = limit_user_ids

    def _select_record(self, url):
        return url.startswith(SEARCH_URL)

    def _item_iter(self, url, json_obj):
        # Ignore error messages
        if isinstance(json_obj, dict) and ('error' in json_obj or 'errors' in json_obj):
            return
        # Search has { "statuses": [tweets] }
        # Timeline has [tweets]
        post_list = json_obj
        for status in post_list:
            yield "text", "post_id", date_parse("time"), "likes"

    @staticmethod
    def item_types():
        return ["facebook_post"]

    def _select_item(self, item):
        if not self.limit_user_ids or item.get("user", {}).get("id_str") in self.limit_user_ids:
            return True
        return False


if __name__ == "__main__":
    FacebookWarcIter.main(FacebookWarcIter)
