import facebook_scraper

for post in facebook_scraper.get_posts(442978589179108, pages = 1):
    print(post['text'][:40])
