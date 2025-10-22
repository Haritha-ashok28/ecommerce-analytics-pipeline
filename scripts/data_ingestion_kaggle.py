# Script to download and extract the Olist Brazilian E-commerce dataset from Kaggle and save it to the specified destination path.
import kaggle
import os
import zipfile

destination_path = os.path.join('d:', 'olist-business-analytics-pipeline', 'data', 'raw_data')

dataset_name = 'olistbr/brazilian-ecommerce'

if not os.path.exists(destination_path):    
    os.makedirs(destination_path)

print(f"Downloading dataset '{dataset_name}' from Kaggle...")
kaggle.api.authenticate()    
kaggle.api.dataset_download_files(dataset_name, path=destination_path, unzip=False)
zip_path = os.path.join(destination_path, 'brazilian-ecommerce.zip')
print(f"Dataset downloaded to {zip_path}")

print("Extracting files...")
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(destination_path)
os.remove(zip_path)
print(f"Files extracted to {destination_path}")

print("Data ingestion completed.")
