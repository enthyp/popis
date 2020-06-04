#!/usr/bin/python3
import csv
import sys
import threading
from multiprocessing import Pool, Queue
from urllib.request import urlopen, Request
from lxml import etree


base_url = 'https://pl.wikiquote.org'
gang_urls = {
    0 : f"{base_url}/wiki/Kategoria:Politycy_PO",
    1 : f"{base_url}/wiki/Kategoria:Politycy_PiS"
}


def find_members(url):
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        
    with urlopen(req) as site:
        site = etree.parse(site, etree.HTMLParser())
        member_tags = site.xpath('//div[@id="bodyContent"]//div[@class="mw-category-group"]/ul/li')
        members = [(
            t.xpath('.//@title')[0], 
            base_url + t.xpath('.//a/@href')[0]
        ) for t in member_tags]
        return members


def member_quotes(name, url, label):
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
       
    with urlopen(req) as site:
        site = etree.parse(site, etree.HTMLParser())
        blocks = site.xpath('//div[@id="bodyContent"]//div[@id="mw-content-text"]//div[@class="mw-parser-output"]')
       
        quotes = []
        for block in blocks:
            points = block.xpath('./*[1]/following-sibling::ul[not(preceding-sibling::h2)]')
            for point in points:
                quote_parts = point.xpath('(./li/text() | ./li/*/text())')
                quote = ''.join(quote_parts)
                quotes.append(quote)

    quotes = [q.strip('\'"\n ') for q in quotes]    
    return quotes, name, label


def write_header(filepath):
    with open(filepath, 'w', newline='') as out_file:
        writer = csv.writer(out_file, delimiter='|')
        writer.writerow(['text', 'labels'])


def write_csv(filepath, q, limit):
    with open(filepath, 'a', newline='') as out_file:
        writer = csv.writer(out_file, delimiter='|')
 
        for i in range(limit):
            result = q.get()
            if result:
                quotes, name, label = result
                writer.writerows((q, label) for q in quotes)
                print(f'Written: {name} {label}\n{limit - i - 1} left')


def main():
    # Header
    filepath = 'popis.csv'
    write_header(filepath)

    # Members
    member_tasks = []
    for label, url in gang_urls.items():
        members = find_members(url)
        member_tasks.extend([(u[0], u[1], label) for u in members])

    # Quotes        
    q = Queue()
    t = threading.Thread(target=write_csv, args=('popis.csv', q, len(member_tasks)))
    t.start()

    def on_result(res):
        q.put(res)

    def on_error(e):
        print('ERROR: ', e)
        q.put(None)

    with Pool(4) as pool:
        for name, url, label in member_tasks:
            h = pool.apply_async(member_quotes, args=(name, url, label), callback=on_result, error_callback=on_error)

        t.join()


if __name__ == '__main__':
    main()

