import requests
import json
import pandas as pd

def get_stats(mbox, assetid):
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
    
    response = requests.post(url, json = params)
    data = json.loads(response.text)
    
    
    columns = {
    'id': [],
    'name': [],
    'assetType': []
    }

    df = pd.DataFrame(columns)

    string = json.loads(data["execute"]["mboxes"][0]["options"][0]["content"])

    columns = {
        'id': [],
        'name': [],
        'assetType': []
    }

    df = pd.DataFrame(columns)

    for key, value in string["adobeRecommendations"]["recDetailedResults"].items():
        values = {
            "id": value["id"],
            "name": value["name"],
            "assetType": value["assetType"]
        }
        value_df = pd.DataFrame([values])
        df = pd.concat([df, value_df], ignore_index=True)
    
    df["name"] = df["name"].str.split('|').str[0]
    #number_name = df["name"].value_counts()
    sum_recommended_properties = number_name = df["name"].nunique()
        
    return sum_recommended_properties    

test_columns = {
    "test_id": [],
    "mbox": [],
    "assetid": [],
    "sum_recommended_properties": []   
}
    
recommended_properties_columns = {
    "recommended_property": [],
    "sum_recommendations": []
}

recommended_assettypes_columns = {
    "recommended_assettype": [],
    "sum_recommendations": []
}
    
df_tests = pd.DataFrame(test_columns)
df_recommended_properties = pd.DataFrame(recommended_properties_columns)
df_recommended_assettypes = pd.DataFrame(recommended_assettypes_columns)

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

test_id = 1
for mbox in mboxes:
    for assetid in assetids:
        sum_recommended_properties = get_stats(mbox, assetid)
        
        entry = {
            "test_id": test_id,
            "mbox": mbox,
            "assetid": assetid,
            "sum_recommended_properties": sum_recommended_properties
        }
        
        df_entry = pd.DataFrame([entry])
        df_tests = pd.concat([df_tests, df_entry], ignore_index=True)
        
        test_id += 1

df_tests = df_tests.sort_values(by=["assetid","mbox"])
print(df_tests)