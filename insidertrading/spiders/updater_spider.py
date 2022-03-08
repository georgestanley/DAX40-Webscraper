import scrapy
from ..items import CompaniesItem, InsiderTradesItem
import mysql.connector
from datetime import datetime,timedelta
from scrapy.crawler import CrawlerProcess


class InsiderSpiderSpider3(scrapy.Spider):
    name='update_spider'
    allowed_domains = ['finanzen.net']

    def start_requests(self):
        self.create_connection()
        self.read_data()
        for company in self.companies:
            #print('Company dict=',company)
            yield scrapy.Request(company['weblink'],callback=self.insider_data,meta={'company_id':company['company_id']})
            #yield scrapy.Request('https://www.finanzen.net/insidertrades/covestro',callback=self.insider_data,meta={'company_id':10})
    
    def read_data(self):
        self.curr.execute("select company_id,insider_trades_weblink from insider_trades.companies")
        results = self.curr.fetchall()
        self.companies = []
        for x in results:
            self.companies.append( {'company_id':x[0],'weblink':x[1]})
        self.curr.execute("select last_executed_at from insider_trades.script_executions where run_id=(select max(run_id) from insider_trades.script_executions);")
        self.last_executed_at = self.curr.fetchall()[0][0]

    def create_connection(self):
        self.conn = mysql.connector.connect(
            host='localhost',
            user = 'root',
            passwd= 'helloworld123',
            database = 'insider_trades'
        )
        self.curr = self.conn.cursor()
    
    def insider_data(self,response):
        #
        #x=response.css(".col-sm-8 td:nth-child(1)::text").extract()
        i_inside_trade =InsiderTradesItem()

        rows=response.xpath('//*[@class="col-sm-8"]//tr')
        #rows = response.xpath('//*[contains(concat( " ", @class, " " ), concat( " ", "col-sm-8", " " ))]')
        #rows = rows.xpath('//*[contains(concat( " ", @class, " " ), concat( " ", "table-responsive", " " ))]//tr')
        l=[]
        for row in rows:
            #print(row)
            try:
                v_date = row.xpath('td[1]//text()').extract_first()
                print(v_date)
                v_date = datetime.strptime(v_date, '%d.%m.%y')
                if v_date > self.last_executed_at:
                    i_inside_trade['company_id']=response.meta.get('company_id')
                    i_inside_trade['date']=v_date
                    i_inside_trade['trader']=row.xpath('td[2]//text()').extract_first()
                    i_inside_trade['quantity']=row.xpath('td[3]//text()').extract_first()
                    i_inside_trade['short_val']=row.xpath('td[4]//text()').extract_first()
                    i_inside_trade['type']=row.xpath('td[5]//text()').extract_first()
                    #print('i_inside_trade=',i_inside_trade)
                    yield i_inside_trade
            except Exception as e :
                print('Error encountered. Pls check')
                print(e)

process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})

#process.crawl(InsiderSpiderSpider3)
#process.start()