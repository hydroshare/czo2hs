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
    nf1, nf2 = [], []
    for f in dg.index:
        while f.endswith('/'):
            f = f[:len(f)-1]
        file_candidate = f.split('/')[-1]
        assert file_candidate, f

        found = glob.glob(os.path.join('/home/mobrien/czo2hs/tmp2/**/', file_candidate))
        if not found:
            likely_file = '.' in f.split('/')[-1]
            for item in ['.htm', '.jsp', '.ml', '.php', '.shtm', '.nc']:
                if item in f.split('/')[-1]:
                    likely_file = False
            if likely_file:
                nf1.append(f)
                print('{} - Not found {}'.format(len(nf1), f))
            else:
                nf2.append(f)
                print('{} - Likely external link {}'.format(len(nf2), f))
            conf_name.append('')
            sizes.append(0)
        else:
            chaff = [x for x in found if "." not in x]
            ffiles = [x for x in found if "." in x]
            if not ffiles:
                print('Found only chaff for {} at {} - qty {}'.format(file_candidate, chaff[:5], len(chaff)))
                conf_name.append('')
                sizes.append(0)
            elif len(ffiles) > 0:
                assert file_candidate == ffiles[0].split('/')[-1], ffiles[0].split('/')[-1]
                conf_name.append(file_candidate)
                sz = os.stat(ffiles[0]).st_size // 1000  # KB
                sizes.append(sz)
                if sz == 0:
                    print('Zero size {}'.format(file_candidate))
            else:
                print('unknown state'.format(file_candidate))
                conf_name.append('')
                sizes.append(0)

    dg['conf'] = conf_name
    dg['sizes'] = sizes
    dg.to_csv('czodata.csv')

    if not os.path.exists('./outputs'):
        os.mkdir('./outputs')

    with open('./outputs/notfound.txt', 'w') as f:
        for item in nf1:
            f.write(item + '\n')
        f.write(str(len(nf1)))

    with open('./outputs/extlinks.txt', 'w') as f:
        for item in nf2:
            f.write(item + '\n')
        f.write(str(len(nf2)))


if __name__ == "__main__":
    start_time = time
    start = time.time()

    try:
        main()
    except KeyboardInterrupt:
        print("\nExit ok")
    finally:
        print("Total Migration {}".format(elapsed_time(start, time.time())))
