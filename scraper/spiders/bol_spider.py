import scrapy, json, os, sqlite3
from types import prepare_class
from datetime import datetime
from scrapy.exceptions import CloseSpider
from scrapy.crawler import CrawlerProcess
from multiprocessing import Process

class BolSpider(scrapy.Spider):
    name = 'bol'
    start_urls = []

    def parse(self, response, counter_web = 1):

        if response.status != 200:
            raise CloseSpider("Error code :{}".format(response.status))

        if response.css('li.product-item--row').get() == None:
             raise CloseSpider("No more products to scrape here...")

        counter = 1
        data = []
        
        for product in response.css('.product-item--row') :

            title = product.css('.product-title::text').get()
            subtitle = product.css('.product-subtitle::text').get()
            price = str(product.xpath('//li[{}]/div[2]/wsp-buy-block/div[1]/section/div/div/meta/@content'.format(counter)).get())
            rating = product.xpath('//li[{}]/div[2]/div/div[3]/div/span'.format(counter)).xpath('@style').get()

            if rating is not None:
                rating = str(rating).split(' ')[1]

            counter += 1
            my_dict = {
                "title": title,
                "subtitle": subtitle,
                "price":  price,
                "rating": rating,
                "date": str(datetime.now()),
            }
            data.append(my_dict)

        pagename = str(response.url).split('/')[5]
        json_filename = "data-{}-bol.json".format(pagename)

        with open(json_filename, 'a') as json_file:
            json.dump(data, json_file, indent = 2)
        json_file.close()

        file = open(json_filename,'r')
        old_data = file.read()
        file.close()
        
        new_data = old_data.replace("][", ",")
        file = open('data_new.json','w')
        file.write(new_data)
        file.close()

        os.remove(json_filename)
        os.rename('data_new.json', json_filename)

        next_page = str(response.url)
        next_page = next_page.split('=')[0] + '=' + str(int(next_page.split('=')[1]) + 1)
        yield scrapy.Request(next_page, callback=self.parse)



def run_spider(url):

    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
    })
    BolSpider.start_urls = []
    BolSpider.start_urls.append(url)
    process.crawl(BolSpider)
    process.start() # the script will block here until the crawling is finished
   

def bol_scraper(urls): # feed a list of URLS

    for url in urls:        
        p = Process(target = run_spider, args=(url,)) 
        p.start()
        p.join()


if __name__ == '__main__':

    urls = [
        #'https://www.bol.com/nl/l/gaming-monitoren/N/10460/filter_N/4274299157/?page=1',     # monitor
        #'https://www.bol.com/nl/l/gaming-toetsenborden/N/18214/?page=1',                    # toetsenbord
        #'https://www.bol.com/nl/l/gaming-muizen/N/18212/?page=1',                           # muis
        #'https://www.bol.com/nl/l/gaming-headsets/N/18210/?page=1',                         # headset
        #'https://www.bol.com/nl/l/laptops/N/4770/?page=1',                                  # laptops
        #'https://www.bol.com/nl/l/game-racestoelen/N/44479/?page=1',                        # game_chair
        #'https://www.bol.com/nl/l/games-voor-de-pc/N/38907/?page=1',                        # pc_games
        #'https://www.bol.com/nl/l/boeken/N/8299/?page=1',                                   # boeken
    ]

    bol_scraper(urls)




