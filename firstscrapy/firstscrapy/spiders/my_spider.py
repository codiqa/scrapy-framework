import scrapy
import pandas as pd
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from pathlib import Path
import re
from firstscrapy.items import FirstscrapyItem

class EINSpider(scrapy.Spider):
    name = "ein_spider"
    FIELDS = [
        "CYTotalRevenueAmt",
        "CYContributionsGrantsAmt",
        "MembershipDuesAmt",
        "GovernmentGrantsAmt",
        "NoncashContributionsAmt",
        "TotalVolunteersCnt"
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.csv_data = {}  # key = EIN, value = CSV row as dict
    
    def start_requests(self):
        # Create folder if it doesn't exist
        Path("CSVs").mkdir(parents=True, exist_ok=True)

        # Start your requests
        url = "https://www.irs.gov/charities-non-profits/exempt-organizations-business-master-file-extract-eo-bmf"
        yield scrapy.Request(url=url, callback=self.parse)
    

    # -------------------------
    # STEP 1: Get CSV links
    # -------------------------
    def parse(self, response):
        csv_links = response.css("a[href*='.csv']::attr(href)").getall()

        for link in csv_links:            
            match = re.match(r".*eo\d.csv$", link)
            if match:
                yield scrapy.Request(url=link, callback=self.parse_csv)

    # -------------------------
    # STEP 2: Parse CSV
    # -------------------------
    def parse_csv(self, response):
        filename = response.url.split("/")[-1]

        # Full path
        file_path = Path("CSVs") / filename

        # Save file
        with open(file_path, "wb") as f:
            f.write(response.body)  # still fast enough for moderate CSVs
        df = pd.read_csv(
            file_path,
            dtype={"EIN": str},
            encoding="latin-1"
        )

        for _,row in df.iterrows():           
            ein = row["EIN"]            
            csvdata= row.to_dict()
            
            url = f"https://projects.propublica.org/nonprofits/organizations/{ein}"

            yield scrapy.Request(
                url=url,
                callback=self.parse_ein,
                meta={"ein": ein, csvdata:csvdata}
            )

    # -------------------------
    # STEP 3: Find XML link
    # -------------------------    
    def parse_ein(self, response):
        soup = BeautifulSoup(response.text, "html.parser")

        tag = soup.find("a", string="XML")
        if not tag:
            return

        xml_url = f"https://projects.propublica.org/{tag.get('href')}"
        
        yield scrapy.Request(
            url=xml_url,
            callback=self.parse_xml,            
            meta={"ein": response.meta["ein"],"csvdata": response.meta["csvdata"]}
        )

    # -------------------------
    # STEP 4: Parse XML
    # -------------------------
    def parse_xml(self, response):
        data = {field: None for field in self.FIELDS}
        data["ein"] = response.meta["ein"]

        try:
            root = ET.fromstring(response.body)

            for elem in root.iter():
                tag = elem.tag.split("}")[-1]
                if tag in self.FIELDS:
                    data[tag] = elem.text

        except Exception as e:
            self.logger.error(f"XML parse error for {data['ein']}: {e}")

        # -------------------------
        # Merge CSV row with XML data
        # -------------------------
        csvdata = response.meta["csvdata"]
        item = FirstscrapyItem()

        merged_data = {**csvdata, **data}

        for k, v in merged_data.items():
            item[k] = v

        yield item  # Scrapy will output merged data