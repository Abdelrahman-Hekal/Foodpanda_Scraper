import time
import os
from datetime import datetime
import pandas as pd
import warnings
import sys
import xlsxwriter
import shutil
import json
import numpy as np
import requests

warnings.filterwarnings('ignore')

def scrape_Foodpanda(output1, page, settings):

    stamp = datetime.now().strftime("%d_%m_%Y")
    data = pd.DataFrame()

    # shop sites
    url = "https://hk.fd-api.com/api/v5/graphql"
    headers = {
    "Accept": "application/json",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en,en-US;q=0.9,ar;q=0.8",
    "Content-Length": "5011",
    "Content-Type": "application/json;charset=UTF-8",
    "Origin": "https://www.foodpanda.hk",
    "Platform": "web",
    "Priority": "u=1, i",
    "Referer": "https://www.foodpanda.hk/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    } 

    shopPayload = {
    "query": """
        fragment FoodLabellingInfoFields on FoodLabellingInfo {
            labelTitle
            labelValues
        }

        fragment FoodLabellingFields on FoodLabelling {
            additives {
                ...FoodLabellingInfoFields
            }
            allergens {
                ...FoodLabellingInfoFields
            }
            nutritionFacts {
                ...FoodLabellingInfoFields
            }
            productClaims {
                ...FoodLabellingInfoFields
            }
            productInfos {
                ...FoodLabellingInfoFields
            }
            warnings {
                ...FoodLabellingInfoFields
            }
        }

        fragment ProductFields on Product {
            attributes(keys: $attributes) {
                key
                value
            }
            badges
            description
            favourite
            foodLabelling {
                ...FoodLabellingFields
            }
            globalCatalogID
            globalCatalogVendorID
            isAvailable
            name
            nmrAdID
            originalPrice
            packagingCharge
            parentID
            price
            productID
            stockAmount
            stockPrediction
            tags
            urls
            vendorID
        }

        fragment ShopItemFields on ShopItem {
            __typename
            ...BannerFields
            ...CategoryFields
            ...ProductFields
        }

        fragment BannerFields on Banner {
            bannerUrl
            globalID
            name
            nmrAdID
        }

        fragment CategoryFields on Category {
            name
            id
            imageUrls
            footerImage
        }

        fragment ShopItemsListFields on ShopItemsList {
            headline
            localizedHeadline
            requestID
            shopItemID
            shopItems {
                ...ShopItemFields
            }
            shopItemType
            swimlaneFilterType
            trackingID
            swimlaneTrackingKey
        }

        fragment PageInfoFields on PageInfo {
            isLast
            pageNumber
        }

        fragment TrackingFields on Tracking {
            experimentID
            experimentVariation
        }

        fragment ShopItemsResponseFields on ShopItemsResponse {
            shopItemsList {
                ...ShopItemsListFields
            }
            pageInfo {
                ...PageInfoFields
            }
            tracking {
                ...TrackingFields
            }
        }

        query getShopDetails(
            $attributes: [String!]
            $globalEntityId: String!
            $isDarkstore: Boolean!
            $locale: String!
            $page: Int! = 0
            $pastOrderStrategy: PastOrderStrategy
            $userCode: String
            $vendorCode: String!
            $includeCategoryTree: Boolean!
            $pageName: String!
            $productIDs: [String!]
            $productSKUs: [String!]
            $complianceLevel: Int!
        ) {
            shopDetails {
                categories(
                    input: {
                        customerID: $userCode
                        globalEntityID: $globalEntityId
                        isDarkstore: $isDarkstore
                        locale: $locale
                        pastOrderStrategy: $pastOrderStrategy
                        platform: "web"
                        vendorID: $vendorCode
                    }
                ) @include(if: $includeCategoryTree) {
                    ...CategoryTreeFields
                }
                shopItemsResponse(
                    complianceLevel: $complianceLevel
                    input: {
                        customerID: $userCode
                        globalEntityID: $globalEntityId
                        isDarkstore: $isDarkstore
                        locale: $locale
                        pastOrderStrategy: $pastOrderStrategy
                        platform: "web"
                        vendorID: $vendorCode
                    }
                    page: $page
                    pageName: $pageName
                    swimlanesProps: {
                        excludeProducts: true
                        productIDs: $productIDs
                        productSKUs: $productSKUs
                    }
                ) {
                    ...ShopItemsResponseFields
                }
            }
        }

        fragment SubCategoryFields on SubCategory {
            id
            name
            productsCount
        }

        fragment CategoryTreeFields on CategoryTree {
            category {
                ...CategoryFields
            }
            productsCount
            subCategories {
                ...SubCategoryFields
                subCategories {
                    ...SubCategoryFields
                    subCategories {
                        ...SubCategoryFields
                    }
                }
            }
        }
    """,
    "variables": {
        "attributes": [
            "baseContentValue",
            "baseUnit",
            "freshnessGuaranteeInDays",
            "maximumSalesQuantity",
            "minPriceLastMonth",
            "pricePerBaseUnit",
            "vatRate",
            "sku",
            "nutri_grade",
            "sugar_level"
        ],
        "complianceLevel": 5,
        "globalEntityId": "FP_HK",
        "includeCategoryTree": False,
        "isDarkstore": False,
        "locale": "en_HK",
        "page": 0,
        "pageName": "shop_detail",
        "vendorCode": ""
        }
    }
    categoryPayload = {
    "query": """
    fragment FoodLabellingInfoFields on FoodLabellingInfo {
        labelTitle
        labelValues
    }

    fragment FoodLabellingFields on FoodLabelling {
        additives {
            ...FoodLabellingInfoFields
        }
        allergens {
            ...FoodLabellingInfoFields
        }
        nutritionFacts {
            ...FoodLabellingInfoFields
        }
        productClaims {
            ...FoodLabellingInfoFields
        }
        productInfos {
            ...FoodLabellingInfoFields
        }
        warnings {
            ...FoodLabellingInfoFields
        }
    }

    fragment ProductFields on Product {
        attributes(keys: $attributes) {
            key
            value
        }
        badges
        description
        favourite
        foodLabelling {
            ...FoodLabellingFields
        }
        globalCatalogID
        globalCatalogVendorID
        isAvailable
        name
        nmrAdID
        originalPrice
        packagingCharge
        parentID
        price
        productID
        stockAmount
        stockPrediction
        tags
        urls
        vendorID
    }

    fragment PageInfoFieldsWithTotalCount on PageInfo {
        isLast
        pageNumber
        totalCount
    }

    query getProducts(
        $attributes: [String!]
        $filters: [ProductFilterInput!]
        $globalEntityId: String!
        $isDarkstore: Boolean!
        $locale: String!
        $page: Int
        $limit: Int
        $userCode: String
        $vendorCode: String!
    ) {
        products(
            input: {
                customerID: $userCode
                filters: $filters
                globalEntityID: $globalEntityId
                isDarkstore: $isDarkstore
                locale: $locale
                page: $page
                limit: $limit
                platform: "web"
                vendorID: $vendorCode
            }
        ) {
            items {
                ...ProductFields
            }
            pageInfo {
                ...PageInfoFieldsWithTotalCount
            }
        }
    }
""",
    "variables": {
        "attributes": [
            "baseContentValue",
            "baseUnit",
            "freshnessGuaranteeInDays",
            "maximumSalesQuantity",
            "minPriceLastMonth",
            "pricePerBaseUnit",
            "vatRate",
            "sku",
            "nutri_grade",
            "sugar_level"
        ],
        "filters": [

        ],
        "globalEntityId": "FP_HK",
        "isDarkstore": False,
        "locale": "en_HK",
        "page": 0,
        "vendorCode": ""
        }
    }

    # get the request arguments
    vendor, categoryId = '', ''
    elems = page.split('/')
    for j, elem in enumerate(elems):
        if elem == "shop" or elem == "darkstore":
            vendor = elems[j+1]
        elif elem == "category":
            categoryId = elems[j+1]

    if not vendor:
        print(f'Error - Unsupported URL: {page}')
        return
    
    if categoryId:
        # one category within a shop
        categoryPayload["variables"]["filters"].append({"type": "Category", "id": categoryId})
        categoryPayload["variables"]["vendorCode"] = vendor
        payload = categoryPayload
        ipage = 0
    else:
        # entire shop or store
        shopPayload["variables"]["vendorCode"] = vendor
        if '/darkstore/' in page:
            shopPayload["variables"]["isDarkstore"] = True
        payload = shopPayload
        ipage = 0

    # scraping Products details
    print('-'*75)
    print('Scraping Products details...')
    print('-'*75)
    iprod = 0
    end = False
    while True:      
        payload["variables"]["page"] = ipage
        for _ in range(10):
            try:
                response = requests.post(url, headers=headers, json=payload)
                if response.status_code == 200:
                    break
                elif response.status_code == 400:
                    end = True
                    break
                else:
                    time.sleep(5)
            except:
                time.sleep(5)        

        if end: break
        ipage += 1
        try:
            details = json.loads(response.text)
        except json.JSONDecodeError as e:
            print(e)

        try:
            if categoryId:
                prods = details["data"]["products"]["items"]
                if prods == None: break
                for prod in prods:
                    iprod += 1
                    print(f'Scraping the details of product {iprod}')
                    row = prod
                    row['productUrl'] = page
                    # row['Product Category'] = category           
                    row['extractionDate'] = stamp
                    # appending the output to the datafame       
                    data = pd.concat([data, pd.DataFrame([row.copy()])], ignore_index=True)
            else:
                elems = details["data"]["shopDetails"]["shopItemsResponse"]["shopItemsList"]
                if not elems: break
                for elem in elems:
                    prods = elem["shopItems"]
                    for prod in prods:
                        if prod['__typename'] != 'Product': continue
                        iprod += 1
                        print(f'Scraping the details of product {iprod}')
                        row = prod
                        row['productUrl'] = page
                        # row['Product Category'] = category           
                        row['extractionDate'] = stamp
                        # appending the output to the datafame       
                        data = pd.concat([data, pd.DataFrame([row.copy()])], ignore_index=True)
        except Exception as err:
            print(f'Warning: the below error occurred while scraping the product link: {page}')
            print(str(err))
           
    # output to excel
    if data.shape[0] > 0:
        try:
            data['extractionDate'] = pd.to_datetime(data['extractionDate'],  errors='coerce', format="%d_%m_%Y")
            data['extractionDate'] = data['extractionDate'].dt.date   
            # Get the list of columns
            data.rename(columns={'productID': 'ID', 'urls': 'imageUrls'}, inplace=True)
            data.drop(['__typename', 'parentID'], axis=1, inplace=True, errors='ignore')
            schemaCols = ["ID", "name", "description", "price", "originalPrice", "packagingCharge", "isAvailable", "stockAmount", "vendorID", "productUrl", "imageUrls", ]
            cols = data.columns
            orderedCols = []
            for col in schemaCols:
                if col in cols:
                    orderedCols.append(col)

            for col in cols:
                if col not in orderedCols:
                    orderedCols.append(col)
            # Reindex the DataFrame with the new column order
            data = data[orderedCols]
            # Replace empty strings with NaN
            data.replace('', np.nan, inplace=True)
            # Replace actual empty list objects with NaN
            data = data.applymap(lambda x: np.nan if x == [] else x)
            # Remove empty columns
            data = data.dropna(axis=1, how='all')

            df1 = pd.read_excel(output1)
            if "extractionDate" in df1.columns:    
                df1['extractionDate'] = df1['extractionDate'].dt.date  
        except:
            pass
        df1 = pd.concat([df1, data], ignore_index=True)
        #df1 = df1.drop_duplicates()
        writer = pd.ExcelWriter(output1, date_format='d/m/yyyy')
        df1.to_excel(writer, index=False)
        writer.close()   
        
               
def get_inputs():
 
    print('-'*75)
    print('Processing The Settings Sheet ...')
    # assuming the inputs to be in the same script directory
    path = os.path.join(os.getcwd(), 'Foodpanda_settings.xlsx')

    if not os.path.isfile(path):
        print('Error: Missing the settings file "Foodpanda_settings.xlsx"')
        input('Press any key to exit')
        sys.exit(1)
    try:
        urls = []
        df = pd.read_excel(path)
        cols  = df.columns
        for col in cols:
            df[col] = df[col].astype(str)

        settings = {}
        inds = df.index
        for ind in inds:
            row = df.iloc[ind]
            link, link_type, status = '', '', ''
            for col in cols:
                if row[col] == 'nan': continue
                elif col == 'Link':
                    link = row[col]
                elif col == 'Scrape':
                    status = row[col]                
                elif col == 'Store / Category':
                    link_type = row[col]                
                else:
                    settings[col] = row[col]

            if link != '': #and status != '' and link_type != '':
                try:
                    #status = int(float(status))
                    #urls.append((link, status, link_type))
                    urls.append(link)
                except:
                    #urls.append((link, 0, link_type))
                    urls.append(link)
    except:
        print('Error: Failed to process the settings sheet')
        input('Press any key to exit')
        sys.exit(1)

    # checking the settings dictionary
    # keys = ["Product Limit"]
    # for key in keys:
    #     if key not in settings.keys():
    #         print(f"Warning: the setting '{key}' is not present in the settings file")
    #         settings[key] = 0
    #     try:
    #         settings[key] = int(float(settings[key]))
    #     except:
    #         input(f"Error: Incorrect value for '{key}', values must be numeric only, press an key to exit.")
    #         sys.exit(1)

    return urls, settings

def initialize_output():

    stamp = datetime.now().strftime("%d_%m_%Y_%H_%M")
    path = os.path.join(os.getcwd(), 'Scraped_Data', stamp)
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)

    file1 = f'Foodpanda_{stamp}.xlsx'

    # Windws and Linux slashes
    output1 = os.path.join(path, file1)

    # Create an new Excel file and add a worksheet.
    workbook1 = xlsxwriter.Workbook(output1)
    workbook1.add_worksheet()
    workbook1.close()        

    return output1

def main():

    print('Initializing The Bot ...')
    start = time.time()
    output1 = initialize_output()
    urls, settings = get_inputs()

    for url in urls:
        #if url[1] == 0: continue
        #link = url[0]
        #cat = url[2]
        link = url
        try:
            scrape_Foodpanda(output1, link, settings)
        except Exception as err: 
            print(f'Warning: the below error occurred:\n {err}')

    print('-'*75)
    elapsed_time = round(((time.time() - start)/60), 4)
    hrs = round(elapsed_time/60, 4)
    input(f'Process is completed in {elapsed_time} mins ({hrs} hours), Press any key to exit.')
    sys.exit()

if __name__ == '__main__':

    main()