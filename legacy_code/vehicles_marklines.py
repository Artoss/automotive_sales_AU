"""
This script scrapes the Marklines website for automotive sales data in Australia by month.

Created/modified: 2025-03-30

Instructions:
1. Install Scrapy: pip install scrapy
2. Save this script as vehicles_marklines.py in the dewr/spiders directory of your Scrapy project.
3. Run the spider using the command: scrapy crawl vehicles_marklines - o output.json
4. The output will be saved in output.json in the same directory as the script.

NOT YET FULLY FUNCTIONAL - NEED TO PARSE THE DATA FROM THE SCRIPT TAGS (REGEX TASK)

"""



import scrapy
import re
import unicodedata


class SalmSpider(scrapy.Spider):
    name = "marklines_vehicles"
    allowed_domains = ["www.marklines.com"]
    start_urls = ["https://www.marklines.com/en/statistics/flash_sales/automotive-sales-in-Australia-by-month"]
    

    def parse(self, response):

        # Automotive Sales in Australia by Month
        title = response.xpath("//h1/text()").get()
        chart_data = response.xpath("//h1/following-sibling::node()")

        # pattern = re.compile(r"<script>(.*?)<\/script>", re.MULTILINE | re.DOTALL)
        # pattern = r"const Data(.*?);"
        # Extract the data used for the main chart from the <script> tags using regex
    #    pattern =  response.xpath("//script[contains(., 'Data')]").re(r"Data(\d{4}) = (\[[\S\s]*?\])")
        pattern =  re.compile(r"Data(\d{4}) = (\[[\S\s]*?\])")
        data = response.xpath("//script[contains(., 'Data')]").re(pattern)

        output = {}
        for indx, datum in enumerate(data):
            if indx%2 == 0: # Loop through every even (incl. zero) item in the data list
                # Nomalize and decode the data string
                # See: https://stackoverflow.com/questions/62127282/python3-how-to-convert-u3000-ideographic-space-to
                alist = [line.strip() for line in data[indx+1].split('\n')]  # Also applies to data[3] and data[5]
                nlist = [unicodedata.normalize('NFKC', line) for line in alist]
                # Clean the list to only return the numbers
                flist = [re.findall(r'\d+', a)[0] for a in nlist if re.findall(r'\d+', a) != []]
                # Convert the clean list into a dictionary using the months (represented as a digit 1-12) as keys
                # and the sales numbers as values
                fdict = {flist[a]: flist[a+1] for a in range(0,len(flist),2) if len(flist[a+1]) > 2}
            output.update({datum: fdict})
        # for indx, datum in enumerate(data):
        #     if indx%2 == 0: # Loop through every even (incl. zero) item in the data list
        #         # Nomalize and decode the data string
        #         # See: https://stackoverflow.com/questions/62127282/python3-how-to-convert-u3000-ideographic-space-to
        #         year = datum[indx]
        #         alist = [line.strip() for line in data[1].split('\n')]  # Also applies to data[3] and data[5]
        #         nlist = [unicodedata.normalize('NFKC', line) for line in alist]
        #         # Clean the list to only return the numbers
        #         flist = [re.findall(r'\d+', a)[0] for a in nlist if re.findall(r'\d+', a) != []]
        #         # Convert the clean list into a dictionary using the months (represented as a digit 1-12) as keys
        #         # and the sales numbers as values
        #         fdict = {flist[a]: flist[a+1] for a in range(0,len(flist),2)}
        #     output = {year: fdict}
    

        data[1] = '\n'.join(nlist)
        data[1] = data[1].replace("'", '"')
        data[1] = data[1].replace("NaN", "null")
        data[1] = data[1].replace("undefined", "null")

        yield {
            "title": title,
            "chart_data": data, 
        }