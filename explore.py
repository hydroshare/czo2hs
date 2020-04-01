import pandas as pd
import time
from utils_logging import text_emphasis, elapsed_time, log_uploaded_file_stats


def main():
    print('hi')
    czo_data = pd.read_csv('./data/CZO-datasets-metadata-2019-10-29.csv')
    df_rows = czo_data[['CZOS', 'COMPONENT_FILES-location$topic$url$data_level$private$doi$metadata_url']]
    data = []
    for k, idx in enumerate(df_rows.index):
        # line = (df_rows['CZOS'][idx], df_rows['COMPONENT_FILES-location$topic$url$data_level$private$doi$metadata_url'][idx].split('|')[2].split('|'))
        fdata = df_rows['COMPONENT_FILES-location$topic$url$data_level$private$doi$metadata_url'][idx].split('|')
        for item in fdata:
            line = (df_rows['CZOS'][idx], item.split('$')[2])
            data.append(line)
    df = pd.DataFrame.from_records(data, columns=['czo', 'files'])
    dg = df.groupby('files').first()
    dg.to_csv('czodata.csv')

    df_links = czo_data['EXTERNAL_LINKS-url$link_text']
    a=1



if __name__ == "__main__":
    start_time = time
    start = time.time()

    try:
        main()
    except KeyboardInterrupt:
        print("\nExit ok")
    finally:
        print("Total Migration {}".format(elapsed_time(start, time.time())))
