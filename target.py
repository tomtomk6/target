import requests
import json
import pandas as pd
import openpyxl

entry_columns = {
    "test_id": [],
    "mbox": [],
    "assetid": [],
    "sum_recommended_properties": [],
    "min_items_per_property": [],
    "max_items_per_property": [],
    "mean_items_per_property": [],
    "median_items_per_property": [],
    "items_per_assettype": [],
}

assetids = [
    "ooyalaID=efaa9cd1-cfea-4e83-8969-d70db6e70c2a",
    "ooyalaID=ea33eb87-f16c-4f42-83e6-35441c622f07",
    "ooyalaID=08d3bcd8-6fd8-434e-8fea-71ce721799b6",
    "ooyalaID=3205403e-418a-4b03-a909-093ab59c7f47",
    "ooyalaID=42a25328-8eb5-4b84-a67c-c246b1a6a504",
    "ooyalaID=519ff64a-9df4-4d6c-a58f-29ca35677664",   
]

mboxes = [
    "recoDataVideos",
    "recoData-3371"
]

def get_df_from_target(mbox, assetid):
    url = "https://rtldisneyfernsehengm.tt.omtrdc.net/rest/v1/delivery?client=rtldisneyfernsehengm&sessionId=d359234570e04f14e1faeeba02d6ab9914e"
    
    params = {
      "context": {
        "channel": "web",
        "browser" : {
          "host" : "demo"
        },
        "address" : {
          "url" : "https://www.toggo.de"
        },
        "screen" : {
          "width" : 1200,
          "height": 1400
        }
      },
        "execute": {
        "mboxes" : [
          {
            "name" : mbox,
            "index" : 0, 
            "parameters" : {
                "entity.id" : assetid
            }
          }
        ]
      },
      "property": {
          "token": "f32451a0-69be-1d1b-e23e-b6130854c9de"
      }
    }

    
    columns = {
        'id': [],
        'property': [],
        'assetType': []
    }

    df_response = pd.DataFrame(columns)
   
    response = requests.post(url, json = params)
    data = json.loads(response.text)
    dataslice = json.loads(data["execute"]["mboxes"][0]["options"][0]["content"])

    for key, value in dataslice["adobeRecommendations"]["recDetailedResults"].items():
        values = {
            "id": value["id"],
            "property": value["name"],
            "assetType": value["assetType"]
        }
        df_values = pd.DataFrame([values])
        df_response = pd.concat([df_response, df_values], ignore_index=True)
    
    df_response["property"] = df_response["property"].str.split('|').str[0]
    
    return df_response

def get_items_per_property(df_target):
    items_per_property = df_target["property"].value_counts()
    df_items_per_property = pd.DataFrame({"property": items_per_property.index, "items": items_per_property.values})
    
    return df_items_per_property
    
def get_sum_recommended_properties(df_target):
    sum_recommended_properties = df_target["property"].nunique()
    
    return sum_recommended_properties

def get_min_max_items_per_property(df_items_per_property):
    result = {
        "min": df_items_per_property["items"].min(),
        "max": df_items_per_property["items"].max()
    }
    
    return result

def get_median_items_per_property(df_items_per_property):
    return df_items_per_property.median(numeric_only=True).values[0]

def get_mean_items_per_property(df_items_per_property):
    return df_items_per_property.mean(numeric_only=True).values[0]

def get_items_per_assettype(df_target):
    items_per_assettype = df_target["assetType"].value_counts()
    df_items_per_assettype = pd.DataFrame({"assettype": items_per_assettype.index, "items": items_per_assettype.values})
    df_items_per_assettype["items"] = df_items_per_assettype["items"].astype(str)
    df_items_per_assettype["combined"] = df_items_per_assettype["assettype"] + "=" + df_items_per_assettype["items"]
    string = "|".join(df_items_per_assettype["combined"].astype(str))
    return string

test_id = 1

df_test_result = pd.DataFrame([entry_columns])
for mbox in mboxes:
    for assetid in assetids:
        df_target = get_df_from_target(mbox, assetid)
        df_items_per_property = get_items_per_property(df_target)
        
        sum_recommended_properties = get_sum_recommended_properties(df_target)
        min_items_per_property = get_min_max_items_per_property(df_items_per_property)["min"]
        max_items_per_property = get_min_max_items_per_property(df_items_per_property)["max"]
        mean_items_per_property = get_mean_items_per_property(df_items_per_property)
        median_items_per_property = get_median_items_per_property(df_items_per_property)
        items_per_assettype = get_items_per_assettype(df_target)
        
                
        entry = {
            "test_id": test_id,
            "mbox": mbox,
            "assetid": assetid,
            "sum_recommended_properties": sum_recommended_properties,
            "min_items_per_property": min_items_per_property,
            "max_items_per_property": max_items_per_property,
            "mean_items_per_property": mean_items_per_property,
            "median_items_per_property": median_items_per_property,
            "items_per_assettype": items_per_assettype
        }
        
        df_entry = pd.DataFrame([entry])
        df_test_result = pd.concat([df_test_result, df_entry], ignore_index=True)
        
        test_id += 1


df_test_result = df_test_result.drop(0)
df_test_result = df_test_result.sort_values(by=["assetid","mbox"])
writer = pd.ExcelWriter('output.xlsx')
df_test_result.to_excel(writer)
writer.save()
