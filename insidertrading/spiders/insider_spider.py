import scrapy
from ..items import CompaniesItem, InsiderTradesItem


class InsiderSpiderSpider(scrapy.Spider):
    name = 'insider_spider'
    allowed_domains = ['finanzen.net']
    start_urls = ['https://www.finanzen.net/index/dax/40-werte/']

    
    
    def parse(self, response):
        # run the code once and get list of all companies in DAX40
        # take the name and attach it against https://www.finanzen.net/insidertrades/<company_name>

        i_companies = CompaniesItem()
        
        companies = response.css(".table-hover td:nth-child(1)")

        for company in companies:
            #print(company)
            i_companies['company_name'] = company.css("a::text").extract_first()
            i_companies['path'] = company.css("a::attr(href)").extract_first()
            modified_path = i_companies['path'].replace('/aktien/','').replace('-aktie','')
            i_companies['insider_trades_weblink'] = "https://www.finanzen.net/insidertrades/"+ modified_path

            
            #yield scrapy.Request( i_companies['insider_trades_weblink'],callback=self.insider_data,meta={'company_name':i_companies['company_name']})
            #yield scrapy.Request('https://www.finanzen.net/insidertrades/airbus',callback=self.insider_data)
            
            yield i_companies

            
        
    def insider_data(self,response):
        #
        #x=response.css(".col-sm-8 td:nth-child(1)::text").extract()
        i_inside_trade =InsiderTradesItem()

        rows=response.xpath('//*[@class="col-sm-8"]//tr')
        l=[]
        for row in rows:
            
            i_inside_trade['company_name']=response.meta.get('company_name')
            i_inside_trade['date']=row.xpath('td[1]//text()').extract_first()
            i_inside_trade['trader']=row.xpath('td[2]//text()').extract_first()
            i_inside_trade['quantity']=row.xpath('td[3]//text()').extract_first()
            i_inside_trade['short_val']=row.xpath('td[4]//text()').extract_first()
            i_inside_trade['type']=row.xpath('td[5]//text()').extract_first()
            print('i_inside_trade=',i_inside_trade)
            yield i_inside_trade
