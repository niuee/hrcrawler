This is a very basic crawler to crawl thoroughbred horse data from the database of netkeiba. 
There was no robots.txt so I kept the requests spacing interval to be 1 sec and so far I haven't have the need to spoof the ip of the crawler. The data crawled is strictly for the mock horse racing game that I am working on not for commercial use. Currently I only collect the horse's name, sex, fur color, born year, it's id on netkeiba, sire, dam.


Use the `crawl_seed` to crawl for starting horses. There are two different approach. 
First route starts with the active horse.
Second route starts with the ranking leaderboard on netkeiba.

There are a few starting horses saved in the crawl list. 
There are curated lists of famous horses at different region in the crawl lists directory.

As of the current state of the crawler there are some exceptions that are not handle data crawled does not follow the norm. 

There is a sql file that runs some of the setup sqls to setup the database to store the data crawled.