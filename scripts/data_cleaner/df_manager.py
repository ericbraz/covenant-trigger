import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from config import path

import pandas as pd

from constants.prospectColumns import PROSPECTS_COLUMNS

class DfManager:
    def __init__(self):
        self.__path = path
        pass

    def open_data(self, relativepath: str, selected_file: str=False, dtype: str=None) -> None | pd.DataFrame:
        all_data = None
        fpath = path(relativepath)

        if os.path.isdir(fpath) and not selected_file:
            all_data = pd.DataFrame()
            for filename in os.listdir(fpath):
                if filename.endswith(".csv"):
                    csv_path = os.path.join(fpath, filename)
                    try:
                        data = pd.read_csv(csv_path, dtype=dtype)
                        all_data = pd.concat([all_data, data], ignore_index=True)
                    except FileNotFoundError:
                        print("Error: File not found - {}".format(relativepath))
                    except pd.errors.ParserError:
                        print("Error: Could not parse CSV - {}".format(relativepath))
        elif selected_file and selected_file.endswith(".csv"):
            csv_path = os.path.join(fpath, selected_file)
            try:
                data = pd.read_csv(csv_path, dtype=dtype)
                all_data = pd.concat([all_data, data], ignore_index=True)
            except FileNotFoundError:
                print("Error: File not found - {}".format(relativepath))
            except pd.errors.ParserError:
                print("Error: Could not parse CSV - {}".format(relativepath))
        else:
            print("Error: '{}' is not a directory.".format(relativepath))
        return all_data

    def save_file_data(self, df, filepathname: str, index=False, float_format=None, sheet_name=None) -> None:
        fpath = path(filepathname)
        success = False
        try:
            if float_format:
                success = df.to_csv(fpath, index=index, float_format=float_format)
            else:
                success = df.to_csv(fpath, index=index)
            print("DataFrame successfully saved as CSV")
        except Exception as e:
            print("Data couldn't be saved as CSV")
            print(e)
        if sheet_name:
            try:
                new_filename = fpath.replace(".csv", ".xlsx")
                df.to_excel(new_filename, sheet_name=sheet_name)
                print("DataFrame successfully saved as Excel")
            except Exception as e:
                print("Data couldn't be saved as Excel")
                print(e)
        
        # If success equals None then it is successful
        if success == None:
            success = True
        return success
    
    def save_ids(self, df, filepathname: str, index=False, float_format=None) -> None:
        fpath = self.__path(filepathname)
        try:
            if float_format:
                df.to_csv(fpath, index=index, float_format=float_format)
            else:
                df.to_csv(fpath, index=index)
        except Exception as e:
            print(e)
    
    def clean_telephones(self, telephone: str) -> str:
        return telephone.replace("+", "").replace(" ", "").replace("-", "")
    
    def remove_file(self, filepathname: str) -> None:
        fpath = path(filepathname)
        os.remove(fpath)

def main():
    # Below an example of the code sending a message
    
    df_manager = DfManager()
    data = df_manager.open_data("/files/apify/")

    selected_data = data[PROSPECTS_COLUMNS].copy()

    selected_data["cid"] = selected_data["cid"].astype(object)
    df_manager.save_file_data(selected_data, "/files/clean/clean_data.csv")

if __name__ == "__main__":
    main()
