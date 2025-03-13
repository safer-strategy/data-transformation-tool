import pandas as pd
import os

class Reader:
    def read_files(self, file_path):
        if file_path.endswith('.xlsx'):
            return pd.read_excel(file_path, sheet_name=None)
        elif file_path.endswith('.csv'):
            if os.path.isdir(file_path):
                data = {}
                for filename in os.listdir(file_path):
                    if filename.endswith('.csv'):
                        tab_name = os.path.splitext(filename)[0]
                        data[tab_name] = pd.read_csv(os.path.join(file_path, filename))
                return data
            else:
                return {os.path.splitext(os.path.basename(file_path))[0]: pd.read_csv(file_path)}
        else:
            raise ValueError("Unsupported file type. Use .xlsx or .csv")