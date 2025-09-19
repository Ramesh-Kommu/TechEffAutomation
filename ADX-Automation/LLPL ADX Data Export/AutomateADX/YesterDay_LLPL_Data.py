from datetime import datetime, timedelta
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
import pandas as pd
import os

cluster = "https://dfazuredataexplorer.westeurope.kusto.windows.net"
database = "dfdataviewer"
username = "jitendra.gupta3@unilever.com"
password = "TeamofCBT@0101#"
today = datetime.today().date()
fixed_time = "00:30:00"
kcsb = KustoConnectionStringBuilder.with_aad_user_password_authentication(cluster, username, password)
print("Connected to ADX")
print("-----------------------------------------------------------")
current_dir = os.path.dirname(os.path.abspath(__file__))
def CreateExcelFile():
    yesterday = datetime.now() - timedelta(days=1)
    filename = "LLPL - ADX vs DS Validation - QA- " + yesterday.strftime("%d-%m-%Y") + ".xlsx"
    folder_path = current_dir+"\\Data"
    file_path = os.path.join(folder_path, filename)
    return file_path
excelfilepath=CreateExcelFile()

def SavetoExcel(data, excelfilepath, zone):
    for col in data.select_dtypes(include=['datetimetz']).columns:
        data[col] = data[col].dt.tz_localize(None)
    if not os.path.exists(excelfilepath):
        with pd.ExcelWriter(excelfilepath, engine='openpyxl') as writer:
            data.to_excel(writer, sheet_name=zone, index=False)
            print(f"{zone} data saved to the excel file with the sheet Named {zone}\n\n")
    else:
        with pd.ExcelWriter(excelfilepath, mode='a', if_sheet_exists='replace', engine='openpyxl') as writer:
            data.to_excel(writer, sheet_name=zone, index=False)
            print(f"{zone} data saved to the excel file with the sheet Named {zone} \n\n")
    
def getKQLData(query,zone):
    print(f"Querying {zone} zone Data")
    client = KustoClient(kcsb)
    response = client.execute(database, query)
    df = pd.DataFrame([row.to_dict() for row in response.primary_results[0]])
    print(f"Saving to the {zone} zone into the excel file")
    SavetoExcel(df,excelfilepath,zone)

start = datetime.combine(today - timedelta(days=1), datetime.strptime(fixed_time, "%H:%M:%S").time())
end = datetime.combine(today, datetime.strptime(fixed_time, "%H:%M:%S").time())
start_str = start.strftime("%Y-%m-%d %H:%M:%S")
end_str = end.strftime("%Y-%m-%d %H:%M:%S")
zones=["PSM","SigmaMixer","Silos","BagInBagOut","Packaging"]
start_time=''
end_time=''
for zone in zones:
    if(zone=="BagInBagOut"):
        start_time="let actual_start  = datetime(\""+start_str+"\");"
        end_time="let end = datetime(\""+end_str+"\");"
    else:
        start_time="let start = datetime(\""+start_str+"\");"
        end_time="let end = datetime(\""+end_str+"\");"
    notepadpath=zone+".txt"
    file_path = current_dir+"\\Queries\\"
    full_path=file_path+notepadpath
    try:
        with open(full_path, 'r', encoding='utf-8') as file:
            contents = file.read()
            multiline_input = f"{start_time}\n{end_time}\n{contents}"
            #print(start_time+"\n"+end_time+"\n"+contents)
            getKQLData(start_time+"\n"+end_time+"\n"+contents,zone)
    except FileNotFoundError:
        print("The file was not found.")
    except Exception as e:
        print(f"An1 error occurred: {e}")
