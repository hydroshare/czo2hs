import pandas as pd
import time
from utils_logging import text_emphasis, elapsed_time, log_uploaded_file_stats
import glob
import os

def main():
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

    conf_name = []
    sizes = []
    for f in  dg.index:
        fn = f.split('/')[-1]
        s = glob.glob(os.path.join('/home/mobrien/czo2hs/tmp2/**/', fn))
        s = [x for x in s if "." in x]
        if not s:
            print('Seemingly not a file {}'.format(fn))
            conf_name.append('')
            sizes.append(0)
        if len(s) > 0:
            conf_name.append(fn)
            sz = os.stat(s[0]).st_size // 1000
            sizes.append(sz)
            if sz == 0:
                print('Zero size {}'.format(fn))
        else:
            print('unknown state'.format(fn))
            conf_name.append('')
            sizes.append(0)

    dg['conf'] = conf_name
    dg['sizes'] = sizes
    dg.to_csv('czodata.csv')


if __name__ == "__main__":
    start_time = time
    start = time.time()

    try:
        main()
    except KeyboardInterrupt:
        print("\nExit ok")
    finally:
        print("Total Migration {}".format(elapsed_time(start, time.time())))
