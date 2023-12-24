import json
import csv
import pandas as pd


def extract_audit_status(j_file):

    browser_info = []
    

    with open(j_file, 'r', encoding='utf-8-sig') as myfile:
        data = myfile.read()
        obj = json.loads(data)
        browsers = obj['response']['browsers']
        # print(browsers)

        for b in browsers:
            browser = b.get('name')
            browser_id = b.get('id')
            browser_info.append((browser_id, browser))

        all_browsers = pd.DataFrame(data=browser_info, columns=['id', 'name'])
        all_browsers.to_csv('browser_xandr.csv',  index=False)
    print(browser_info)


def main():
    j_file = 'browsers.json'
    file_path = '/Users/maksym/Desktop/{0}'.format(j_file)

    extract_audit_status(file_path)


if __name__ == '__main__':
    main()
