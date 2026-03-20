# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import csv

class FirstscrapyPipeline:
    def open_spider(self, spider):
        # Open the CSV file in write mode
        self.file = open("output.csv", "w", newline='', encoding="utf-8")
        self.writer = None  # we'll initialize DictWriter later

    def close_spider(self, spider):
        # Close the file when spider finishes
        self.file.close()

    def process_item(self, item, spider):
        # Convert Item to dict
        item_dict = dict(item)

        # Initialize DictWriter with headers from first item
        if self.writer is None:
            fieldnames = list(item_dict.keys())
            self.writer = csv.DictWriter(self.file, fieldnames=fieldnames)
            self.writer.writeheader()  # write CSV header

        # Write the item
        self.writer.writerow(item_dict)
        return item 
