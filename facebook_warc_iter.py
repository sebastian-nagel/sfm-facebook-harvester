#!/usr/bin/env python3

from __future__ import absolute_import
from sfmutils.warc_iter import BaseWarcIter, log, IterItem
from dateutil.parser import parse as date_parse
import json
import sys
import logging
import os
from warcio.archiveiterator import WARCIterator
from collections import namedtuple

SEARCH_URL = "https://m.facebook.com/"

class FacebookWarcIter(BaseWarcIter):
    def __init__(self, filepaths, limit_user_ids=None):
        BaseWarcIter.__init__(self, filepaths)
        self.limit_user_ids = limit_user_ids

    def iter(self, limit_item_types=None, dedupe=False, item_date_start=None, item_date_end=None):
        """
        Function is simply copied from sfmutils.warc_iter but adapted to also iterate over
        warc record types of type 'meta' as the previous function only considered type 'response'
        :return: Iterator returning IterItems.
        """
        seen_ids = {}
        for filepath in self.filepaths:
            log.info("Iterating over %s", filepath)
            filename = os.path.basename(filepath)

            with open(filepath, 'rb') as f:
                yield_count = 0

                for record_count, record in enumerate((r for r in WARCIterator(f) if r.rec_type == 'metadata')):
                    self._debug_counts(filename, record_count, yield_count, by_record_count=True)
                    record_url = record.rec_headers.get_header('WARC-Target-URI')
                    record_id = record.rec_headers.get_header('WARC-Record-ID')
                    if self._select_record(record_url):
                        stream = record.content_stream()
                        line = stream.readline().decode('utf-8')
                        while line:
                            json_obj = None
                            try:
                                if line != "\r\n":
                                    # A non-line-oriented payload only has one payload part.
                                    json_obj = json.loads(line)
                            except ValueError:
                                log.warning("Bad json in record %s: %s", record_id, line)
                            if json_obj:
                                for item_type, item_id, item_date, item in self._item_iter(record_url, json_obj):
                                    # None for item_type indicates that the type is not handled. OK to ignore.
                                    if item_type is not None:
                                        yield_item = True
                                        if limit_item_types and item_type not in limit_item_types:
                                            yield_item = False
                                        if item_date_start and item_date and item_date < item_date_start:
                                            yield_item = False
                                        if item_date_end and item_date and item_date > item_date_end:
                                            yield_item = False
                                        if not self._select_item(item):
                                            yield_item = False
                                        if dedupe and yield_item:
                                            if item_id in seen_ids:
                                                yield_item = False
                                            else:
                                                seen_ids[item_id] = True
                                        if yield_item:
                                            if item is not None:
                                                yield_count += 1
                                                self._debug_counts(filename, record_count, yield_count,
                                                                   by_record_count=False)
                                                yield IterItem(item_type, item_id, item_date, record_url, item)
                                            else:
                                                log.warn("Bad response in record %s", record_id)
                            line = stream.readline().decode('utf-8')


    def _select_record(self, url):
        return True

    def _item_iter(self, url, json_obj):

        # Ignore error messages
        if isinstance(json_obj, dict) and ('error' in json_obj or 'errors' in json_obj):
            log.info("Error in json payload of %s", json_obj)
            return


        post_list = json_obj

        if "post_id" in post_list[0]:

            for status in post_list:
                yield "facebook_status", status["post_id"], status["time"], status

        elif "created_at" in post_list:

                yield "facebook_bio",  "not given for fb bio", "not given for fb bio", post_list

    @property
    def line_oriented(self):
        return True

    @staticmethod
    def item_types():
        return ["facebook_status"] # ["facebook_post"]

#    def _select_item(self, item):
#        if not self.limit_user_ids or item.get("user", {}).get("id_str") in self.limit_user_ids:
#            return True
#        return False



if __name__ == "__main__":
    FacebookWarcIter.main(FacebookWarcIter)
