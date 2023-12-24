import pandas as pd
import tldextract
import re


def extract_url(data):
    '''tldextract is a library for extracting info from URLS,
    regex alternative
    s = 'ratsit.se (806716)'
    s2 = re.sub(r'[^aA-zZ.]+', '', s)
    '''
    data1 = pd.read_csv(data)
    data_frame = pd.DataFrame(data=data1)
    url_list = []
    # print(len(data_frame))
    for index, row in data_frame.iterrows():
        row = row['url']  # [7:] need to think of regex + how to extract domain/sub
        no_cache_extract = tldextract.TLDExtract(cache_file=False)
        url = no_cache_extract(row)
        url_list.append(url.registered_domain)
    # print(len(url_list))

    data_new = pd.DataFrame(data=url_list)
    data_new.to_csv('urls.csv', header=False, index=False)


def main():
    to_do_table = 'verify.csv'
    extract_url(to_do_table)

if __name__ == '__main__':
    main()
