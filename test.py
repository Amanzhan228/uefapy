import pandas as pd
import os

csv_folder = "csv"

for file in os.listdir(csv_folder):
    if file.endswith(".csv"):
        csv_file = os.path.join(csv_folder, file)
        print("Читаем:", csv_file)
        df = pd.read_csv(csv_file)
        print(df.head())
