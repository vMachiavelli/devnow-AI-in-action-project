import pandas as pd
from pymongo import MongoClient

# Mongo config
CSV_FILE = "survey_results_public.csv"
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "dev_trends"
COLLECTION_NAME = "developer_profiles"
LIMIT = None  # set to None to load everything

# load csv file for importing
df = pd.read_csv(CSV_FILE)

# === STEP 2: SELECT & RENAME RELEVANT COLUMNS ===
fields_map = {
    'DevType': 'role',
    'ToolsTechHaveWorkedWith': 'tools_used',
    'LanguageHaveWorkedWith': 'languages',
    'PlatformHaveWorkedWith': 'platforms',
    'WebframeHaveWorkedWith': 'frameworks',
    'Country': 'country',
    'WorkExp': 'experience',
    'EdLevel': 'education',
    'Age': 'age'
}
df = df[list(fields_map.keys())].rename(columns=fields_map)

# clean csv
def split_semi_colon(val):
    if pd.isna(val):
        return []
    return [item.strip() for item in val.split(';') if item.strip()]

for col in ['role', 'tools_used', 'languages', 'platforms', 'frameworks']:
    df[col] = df[col].apply(split_semi_colon)

df = df[df['role'].apply(lambda r: len(r) > 0)]

if LIMIT:
    df = df.head(LIMIT)

docs = df.to_dict(orient='records')

# insert records into db
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

insert_result = collection.insert_many(docs)
print(f"✅ Inserted {len(insert_result.inserted_ids)} documents into {DB_NAME}.{COLLECTION_NAME}")
