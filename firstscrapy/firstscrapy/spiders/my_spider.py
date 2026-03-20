import scrapy
import pandas as pd
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from pathlib import Path

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
    
    def handle_error(self, failure):
        import pdb; pdb.set_trace()
    
    def start_requests(self):
        # xml_url = f"https://projects.propublica.org/{tag.get('href')}"
        xml_url = 'https://projects.propublica.org/nonprofits/download-xml?object_id=202513449349301371'

        print( xml_url )
        
        try:
            yield scrapy.Request(
                url=xml_url,
                callback=self.parse_xml,
                errback=self.handle_error,
                meta={"ein": '981644897'},
                dont_filter=True,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                    "Accept": "application/xml,text/html;q=0.9,*/*;q=0.8",
                }
            )
        except Exception as e:
            import pdb; pdb.set_trace()
        
        # # Create folder if it doesn't exist
        # Path("CSVs").mkdir(parents=True, exist_ok=True)

        # # Start your requests
        # url = "https://www.irs.gov/charities-non-profits/exempt-organizations-business-master-file-extract-eo-bmf"
        # yield scrapy.Request(url=url, callback=self.parse)
    

    # -------------------------
    # STEP 1: Get CSV links
    # -------------------------
    def parse(self, response):
        csv_links = response.css("a[href*='.csv']::attr(href)").getall()

        for link in csv_links:
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
            self.csv_data[ein] = row.to_dict()  # store row for later merge 
            url = f"https://projects.propublica.org/nonprofits/organizations/{ein}"

            yield scrapy.Request(
                url=url,
                callback=self.parse_ein,
                meta={"ein": ein}
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

        print( xml_url )
        self.logger.error(f"{xml_url}")
        yield scrapy.Request(
            url=xml_url,
            callback=self.parse_xml,
            meta={"ein": response.meta["ein"]}
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
        csv_row = self.csv_data.get(data["ein"], {})  # get CSV row by EIN
        merged = {**csv_row, **data}      # merge dictionaries

        yield merged  # Scrapy will output merged data