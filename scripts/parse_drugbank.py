
import xml.etree.ElementTree as ET
import pandas as pd
import os

# -----------------------------
# 1. Set file paths
# -----------------------------
XML_PATH = "../data/drugbank.xml"   # path to your XML
OUTPUT_PREFIX = "../output/drug_interactions_chunk"
CHUNK_SIZE = 1000000                # Excel-friendly row limit

# -----------------------------
# 2. Parse XML
# -----------------------------
print("Loading XML file...")
tree = ET.parse(XML_PATH)
root = tree.getroot()

# DrugBank uses XML namespace
namespace = {'db': 'http://www.drugbank.ca'}

# -----------------------------
# 3. Extract interactions
# -----------------------------
interactions = []

for drug in root.findall("db:drug", namespace):
    drug_1_id_elem = drug.find("db:drugbank-id[@primary='true']", namespace)
    drug_1_name_elem = drug.find("db:name", namespace)

    if drug_1_id_elem is None or drug_1_name_elem is None:
        continue

    drug_1_id = drug_1_id_elem.text
    drug_1_name = drug_1_name_elem.text

    drug_interactions = drug.find("db:drug-interactions", namespace)
    if drug_interactions is None:
        continue

    for interaction in drug_interactions.findall("db:drug-interaction", namespace):
        drug_2_id_elem = interaction.find("db:drugbank-id", namespace)
        drug_2_name_elem = interaction.find("db:name", namespace)
        description_elem = interaction.find("db:description", namespace)
        severity_elem = interaction.find("db:severity", namespace)

        if drug_2_id_elem is None or drug_2_name_elem is None or description_elem is None:
            continue

        interactions.append({
            "drug_1_id": drug_1_id,
            "drug_1_name": drug_1_name,
            "drug_2_id": drug_2_id_elem.text,
            "drug_2_name": drug_2_name_elem.text,
            "description": description_elem.text,
            "severity": severity_elem.text if severity_elem is not None else ""
        })

print(f"Total interactions extracted: {len(interactions)}")

# -----------------------------
# 4. Convert to DataFrame
# -----------------------------
df = pd.DataFrame(interactions)

# -----------------------------
# 5. Save as CSV chunks
# -----------------------------
os.makedirs("../output", exist_ok=True)

for i, start in enumerate(range(0, len(df), CHUNK_SIZE)):
    df_chunk = df.iloc[start:start+CHUNK_SIZE]
    filename = f"{OUTPUT_PREFIX}_{i+1}.csv"
    df_chunk.to_csv(filename, index=False)
    print(f"Saved {filename} with {len(df_chunk)} rows")

print("All CSV chunks created successfully!")
