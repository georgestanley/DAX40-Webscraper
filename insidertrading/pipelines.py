# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import mysql.connector


class InsidertradingPipeline:

    def __init__(self) -> None:
        self.create_connection()
        self.create_table()

        
    def create_connection(self):
        self.conn = mysql.connector.connect(
            host='localhost',
            user = 'root',
            passwd= 'helloworld123',
            database = 'insider_trades'
        )
        self.curr = self.conn.cursor()

    def create_table(self):
        self.curr.execute("""DROP TABLE IF EXISTS companies""")
        self.curr.execute("""create table companies(
            company_name text,
            path text,
            insider_trades_weblink text)""")
    

    def process_item(self, item, spider):
        self.store_db(item)
        print('Pipeline:'+ item['company_name'][0] )
        return item

    def store_db(self, item):
        self.curr.execute("""insert into companies values (%s,%s,%s)""",
        (
            item['company_name'],
            item['path'],
            item['insider_trades_weblink']

        ) )
        self.conn.commit()
