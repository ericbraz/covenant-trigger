import pandas as pd
from scripts.data_cleaner.df_manager import DfManager

if __name__ == "__main__":
    df_manager = DfManager()
    temp_file = df_manager.open_data("/files/sent/", selected_file="sent_temp.csv")

    if isinstance(temp_file, pd.DataFrame):
        sent = df_manager.open_data("/files/sent/", selected_file="sent.csv")
        final = pd.concat([sent, temp_file])
        df_manager.save_file_data(final, "/files/sent/sent.csv", float_format="%.0f")
        df_manager.remove_file("/files/sent/sent_temp.csv")
