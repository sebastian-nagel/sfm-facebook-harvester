# Facebook Harvester for SFM

Social Feed Manager (SFM) harvests social media data from multiple platforms' public APIs to help archivists,
librarians, and researchers to build social media collections. [More information about the project itself.](http://gwu-libraries.github.io/sfm-ui). [Main repo here](https://github.com/gwu-libraries/sfm-ui/).

This harvester can be integrated as part of SFM. It accesses facebook public pages (not friend pages) and is able to retrieve:

* posts (post text, link, date...)
* info in bio (creation data, likes...)
* ads (*under development*)

# To Do's

* implement sleep between requests
* what to do in case of blocking
* gzip options in filenames
* more options for scraping adjustable
* deal with 'not found' facebook sites
