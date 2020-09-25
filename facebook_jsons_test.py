import facebook_scraper
import json
import datetime
from warcio.warcwriter import WARCWriter
from warcio.statusandheaders import StatusAndHeaders
import requests
from io import BytesIO

with open('example.warc.gz', 'wb') as output:
    writer = WARCWriter(output, gzip=True)

    resp = requests.get('http://example.com/',
                        headers={'Accept-Encoding': 'identity'},
                        stream=True)

    # get raw headers from urllib3
    headers_list = resp.raw.headers.items()

    http_headers = StatusAndHeaders('200 OK', headers_list, protocol='HTTP/1.0')
    print(resp.raw)
    record = writer.create_warc_record('http://example.com/', 'response',
                                        payload=resp.raw,
                                        http_headers=http_headers)

    writer.write_record(record)

#quit()

all_posts = []

for post in facebook_scraper.get_posts(442978589179108, extra_info = True, pages = 1, timeout = 20):
    print(post['text'][:40])
    all_posts.append(post)

print(all_posts)

def json_date_converter(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()

#write additional warc that records the json payload
with open("fb_test", "wb") as result_warc_file:
    writer = WARCWriter(result_warc_file, gzip = False)

    json_payload = json.dumps(all_posts, default = json_date_converter).encode("utf-8")

    record = writer.create_warc_record('https:://example.com', 'metadata',
                                        payload = BytesIO(json_payload),
                                        warc_content_type = "application/json")
    writer.write_record(record)
#    log.info("Writing scraped results to %s", self.warc_temp_dir)
