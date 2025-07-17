## fresh paste of file cleaner

from config.paths import DIRTY_FILES_DIR, CLEAN_FILES_DIR
import os
import pandas as pd

STANDARD_COLUMNS = [
    'SKU', 
    'Product URL',
 	'MAP',
    'Price',
    'Seller',
    'PDF'
    ]

class FileCleaner():
    def __init__(self):
        self.clean_files = {}
        self.file_paths = []
    
    def run(self):
        self._capture_files()
        self.export_clean_files()

    def _capture_files(self):
        for filename in os.listdir(DIRTY_FILES_DIR):
            file_path = os.path.join(DIRTY_FILES_DIR, filename)

            if not filename.lower().endswith(".csv"):
                continue

            if os.path.isfile(file_path):
                df = pd.read_csv(file_path)

                # add to file paths to be archived later
                self.file_paths.append(file_path)

                try:
                    df_clean = self._truncate_dataframe(df)
                    name = self._extract_name(df_clean['Seller'])
                    self.clean_files[name] = df_clean
                except Exception as e:
                    print(f"Skipping {filename}: {e}")

    @staticmethod
    def _extract_name(names: pd.Series) -> str:
        '''
        Extracts name from a specified column, 
        Return single name
        '''
        unique_names = names.dropna().unique()
        if len(unique_names) != 1:
            raise ValueError("Multiple or no sellers found!")
        
        return unique_names[0]

    @staticmethod
    def _truncate_dataframe(df: pd.DataFrame):
        '''
        Reorganize columns and return only pertinent columns
        '''
        missing = [col for col in STANDARD_COLUMNS if col not in df.columns]
        
        if missing:
            raise ValueError(f"missing columns! {missing}")

        return df[STANDARD_COLUMNS]

    def export_clean_files(self):
        if not os.path.exists(CLEAN_FILES_DIR):
            os.makedirs(CLEAN_FILES_DIR)

        for name, df in self.clean_files.items():
            file_name = f"{name}.csv"
            export_path = os.path.join(CLEAN_FILES_DIR, file_name)
            df.to_csv(export_path, index=False)
            print(f"Exported: {export_path}")