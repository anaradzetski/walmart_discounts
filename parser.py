import json
from functools import lru_cache

import requests
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup as bs

from utils import json_dive

FLASH_PICKS_URL_PATTERN = "https://www.walmart.com/shop/deals/flash-picks?affinityOverride=default{page_str}"

HEADERS = {
    "Host": "www.walmart.com",
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:97.0) Gecko/20100101 Firefox/97.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Cookie": 'akavpau_p2=1649302886~id=9c6889f6ce25be30079a4b81f8b76168; ak_bmsc=92C240CE1C038619AFAC348F42117559~000000000000000000000000000000~YAAQhcQTAvoAiwGAAQAAAlv4AQ97QmdtDyXDs27pdpaUrqSAux6q2yus6O+Q/e6uJlpbWRBVE+3+5DDg3saDbwoXu9Md736qTwecbCudcfuXXTZl6Q/YHQQuPtSCBlxHDzdDYXebLQ2DBy6zSQQa6bAskbTMETKgN1C2QcTdoyq3Vv6ioLPNOkmB2n0Ul263G+FAchzQWBuje/H6lotRGzclNMr60Z/Ch2gLFwC5vv43jqmM37j2cD5XrlPOfqW6gEUTKJicIZ3XiVK651qdJxSk4mZb6LAJWXeAkjeYv9sVnQxCM1fvGCkQkRMFo2XprRZ6yxK0gnsfAqYAtW1EocD4nqw2AlPvZsd17q+6YQClamOpWJQ02fy6nE7IYCiCNxP88S8odcXP1XagDcHXg/iSLjv0M5uSiV9LWXjyYY7L+QCGJWUHi//nJvKTgn60LGBnMByjzPEuYHDOBVIx0jfeoOgyUHM0HcSCCHy1FWjpTCsZjj5VJYz1oxs=; TBV=7; pxcts=08cce94a-b61f-11ec-b29b-6c70765a5959; _pxvid=08ccde88-b61f-11ec-b29b-6c70765a5959; adblocked=false; dimensionData=888; ACID=9e66cdc9-ff59-4f53-910b-fcac2994ee39; hasACID=true; assortmentStoreId=3081; hasLocData=1; TB_Latency_Tracker_100=1; TB_Navigation_Preload_01=0; TB_SFOU-100=; vtc=SoMREuhYfn-dazQg7N3UxU; bstc=SoMREuhYfn-dazQg7N3UxU; mobileweb=0; xpa=; xpm=7%2B1649300495%2B~%2B0; xptwg=74740548:18F6113B02AF570:410072A:7D334AB5:C45C0C4A:2CC3DB35:; TS01b0be75=01538efd7ca5706a5f044a92d235395497116369026f42f11a30e8c1fe8e77911000d2c53c1e09f65cdee6d1b4b69c4b5207e50315; TS013ed49a=01538efd7cfc4b1b4b9a9bbb859492fe065112ea8ec734f5ccf5c5b28f2b6031cfe82b363dbffaae5b575dec2709a87735e91d9faa; bm_sv=BB95DCED0C6E2C72C52B27F0128301B1~FPkhTlUuPTeNM+Ygq0pnvmAr/KB4YmdTj2J4hIXtZy5bFNbAU0+/1DMMjyQCk8Q6Dd1Yf17w4oOunwTRR2qRF4WJOgnF6XWyptjCDYICL1qH9J8zFftNCQvphFkku7FYDOBr8WGCvYzCeSDHzu8Pq4rE38ERKWtdWevQqWhBTTg=; locDataV3=eyJpc0RlZmF1bHRlZCI6dHJ1ZSwiaW50ZW50IjoiU0hJUFBJTkciLCJwaWNrdXAiOlt7ImJ1SWQiOiIwIiwibm9kZUlkIjoiMzA4MSIsImRpc3BsYXlOYW1lIjoiU2FjcmFtZW50byBTdXBlcmNlbnRlciIsIm5vZGVUeXBlIjoiU1RPUkUiLCJhZGRyZXNzIjp7InBvc3RhbENvZGUiOiI5NTgyOSIsImFkZHJlc3NMaW5lMSI6Ijg5MTUgR2VyYmVyIFJvYWQiLCJjaXR5IjoiU2FjcmFtZW50byIsInN0YXRlIjoiQ0EiLCJjb3VudHJ5IjoiVVMiLCJwb3N0YWxDb2RlOSI6Ijk1ODI5LTAwMDAifSwiZ2VvUG9pbnQiOnsibGF0aXR1ZGUiOjM4LjQ4MjY3NywibG9uZ2l0dWRlIjotMTIxLjM2OTAyNn0sImlzR2xhc3NFbmFibGVkIjp0cnVlLCJzY2hlZHVsZWRFbmFibGVkIjp0cnVlLCJ1blNjaGVkdWxlZEVuYWJsZWQiOnRydWUsImh1Yk5vZGVJZCI6IjMwODEifV0sInNoaXBwaW5nQWRkcmVzcyI6eyJsYXRpdHVkZSI6MzguNDgyNjc3LCJsb25naXR1ZGUiOi0xMjEuMzY5MDI2LCJwb3N0YWxDb2RlIjoiOTU4MjkiLCJjaXR5IjoiU2FjcmFtZW50byIsInN0YXRlIjoiQ0EiLCJjb3VudHJ5Q29kZSI6IlVTIiwibG9jYXRpb25BY2N1cmFjeSI6ImxvdyIsImdpZnRBZGRyZXNzIjpmYWxzZX0sImFzc29ydG1lbnQiOnsibm9kZUlkIjoiMzA4MSIsImRpc3BsYXlOYW1lIjoiU2FjcmFtZW50byBTdXBlcmNlbnRlciIsImFjY2Vzc1BvaW50cyI6bnVsbCwic3VwcG9ydGVkQWNjZXNzVHlwZXMiOltdLCJpbnRlbnQiOiJQSUNLVVAiLCJzY2hlZHVsZUVuYWJsZWQiOmZhbHNlfSwiZGVsaXZlcnkiOnsiYnVJZCI6IjAiLCJub2RlSWQiOiIzMDgxIiwiZGlzcGxheU5hbWUiOiJTYWNyYW1lbnRvIFN1cGVyY2VudGVyIiwibm9kZVR5cGUiOiJTVE9SRSIsImFkZHJlc3MiOnsicG9zdGFsQ29kZSI6Ijk1ODI5IiwiYWRkcmVzc0xpbmUxIjoiODkxNSBHZXJiZXIgUm9hZCIsImNpdHkiOiJTYWNyYW1lbnRvIiwic3RhdGUiOiJDQSIsImNvdW50cnkiOiJVUyIsInBvc3RhbENvZGU5IjoiOTU4MjktMDAwMCJ9LCJnZW9Qb2ludCI6eyJsYXRpdHVkZSI6MzguNDgyNjc3LCJsb25naXR1ZGUiOi0xMjEuMzY5MDI2fSwiaXNHbGFzc0VuYWJsZWQiOnRydWUsInNjaGVkdWxlZEVuYWJsZWQiOnRydWUsInVuU2NoZWR1bGVkRW5hYmxlZCI6dHJ1ZSwiYWNjZXNzUG9pbnRzIjpbeyJhY2Nlc3NUeXBlIjoiREVMSVZFUllfQUREUkVTUyJ9XSwiaHViTm9kZUlkIjoiMzA4MSIsInN1cHBvcnRlZEFjY2Vzc1R5cGVzIjpbIkRFTElWRVJZX0FERFJFU1MiXX0sImluc3RvcmUiOmZhbHNlLCJyZWZyZXNoQXQiOjE2NDkzMjIwOTU4ODIsInZhbGlkYXRlS2V5IjoicHJvZDp2Mjo5ZTY2Y2RjOS1mZjU5LTRmNTMtOTEwYi1mY2FjMjk5NGVlMzkifQ%3D%3D; locGuestData=eyJpbnRlbnQiOiJTSElQUElORyIsInN0b3JlSW50ZW50IjoiUElDS1VQIiwibWVyZ2VGbGFnIjpmYWxzZSwiaXNEZWZhdWx0ZWQiOnRydWUsInBpY2t1cCI6eyJub2RlSWQiOiIzMDgxIiwidGltZXN0YW1wIjoxNjQ5MzAwNDk1ODc2fSwicG9zdGFsQ29kZSI6eyJ0aW1lc3RhbXAiOjE2NDkzMDA0OTU4NzYsImJhc2UiOiI5NTgyOSJ9LCJ2YWxpZGF0ZUtleSI6InByb2Q6djI6OWU2NmNkYzktZmY1OS00ZjUzLTkxMGItZmNhYzI5OTRlZTM5In0%3D; tb_sw_supported=false; com.wm.reflector="reflectorid:0000000000000000000000@lastupd:1649302381000@firstcreate:1649301589670"; _gcl_au=1.1.1265595479.1649301265; ndcache=d; __gads=ID=a63a34f7f6e392e6:T=1649301267:S=ALNI_Mb9Z0qSLWYBQQmf_8mHyVcMAcas8A; _uetsid=d3ecc7a0b62011eca1b8cb066ceb7bf0; _uetvid=d3eccc80b62011ec8906478f7d374ae9; _px3=d4ec6685e1c36725549f47c0951f7a91423ac20a88558dce65de464ede0f6e35:oiPdD+pH1G+x1Wt/hfwfOj9797ZpSqwqiJWd0xsaA+VV1JBSn50j/Pu+xPi/CgHFV5Z0sqpeFfFaIXrgNEeqIQ==:1000:MvPMHV8uCjd/4XpWlnfn7glIKA8lFs09Ho2ztmAILucMhwYM6AEGTrgs0TvkccAC/1FDvwpqfu6fPPEA7HfIe0YXEF4mgWfhv6lvdwvi/Uvi7SVDSlrdsm/Qxyu5pPlTGqKYxjq0UPGLJCnmiQ5LDI828M9sBLzR1o+bzGbEk7jcFQQ7BcfE2Kn/gbc32R2vtiX6hcd6n9AGEK1zuLprfg==; _pxde=80e30de13c57ecf1c75b8381102a7c6ab29faf9f67a8c8d1bd61488b17748393:eyJ0aW1lc3RhbXAiOjE2NDkzMDIyODU2OTIsImZfa2IiOjAsImlwY19pZCI6W119',
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0"
}

@lru_cache()
def load_page(page=1):
    page_str = f"&page={page}" if page != 1 else ""
    return bs(
        requests.get(url=FLASH_PICKS_URL_PATTERN.format(page_str=page_str), headers=HEADERS).text,
        "html.parser"
    )

def get_main_json(page):
    return json.loads(load_page(page).body.find(id="__NEXT_DATA__").text)

def get_number_of_pages():
    main_json = get_main_json(1)
    return json_dive(
        main_json,
        [
            "props", "pageProps", "initialData",
             "searchResult", "paginationV2", "maxPage"
        ]
    )

def get_items(page):
    main_json = get_main_json(page)
    return json_dive(
        main_json,
        [
            "props", "pageProps", "initialData",
            "searchResult", "itemStacks", 0, "items"
        ]
    )

PROPERTY_RULES = {
    "Name": ["name"],
    "CurrentPrice": ["priceInfo", "linePrice"],
    "PreviousPrice": ["priceInfo", "wasPrice"],
    "Rating": ["rating", "averageRating"]
}

def parse():
    records = []
    for page in tqdm(range(1, get_number_of_pages() + 1)):
        items = get_items(page)
        for item in items:
            record = {}
            failure = False
            try:
                for prop, rule in PROPERTY_RULES.items():
                    prop_value = json_dive(item, rule)
                    if type(prop_value) is str and len(prop_value) == 0:
                        failure = True
                    record[prop] = prop_value
            except KeyError:
                continue
            if failure:
                continue
            record["URL"] = "https://www.walmart.com" + item["canonicalUrl"]
            records.append(record)
    return pd.DataFrame(records)
