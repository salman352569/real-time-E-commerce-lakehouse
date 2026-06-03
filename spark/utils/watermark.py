import json 

def get_watermark():
   
   with open(
      "metadata/watermark.json",
      "r"
   ) as f :
      return json.load(f)[
           "last_processed_timestamp"] 
   

def update_watermark(timestamp):
   with open(
      "metadata/watermark.json",
      "w"
   )as f:
      
      json.dump(
         {
            "last_processed_timestamp":
            timestamp
         },
         f
      )