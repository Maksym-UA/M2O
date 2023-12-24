import json
import pandas as pd
import clean_csv as cc
import re


def create_df(data):
    '''Convert csv file into Pandas dataframe'''

    provided_data = pd.read_csv(data)
    dataframe = pd.DataFrame(data=provided_data)
    # print(data_frame.head(1).T) # transposed
    print('Dataframe created')
    return dataframe


def read_inventory_file(file):
    ''' Read a .csv file with inventory 
    '''
    all_inventory = []
    with open(file) as f:
        read_data = f.readlines()
        for i in read_data:
            new_inventory = i.rstrip('\n')
            all_inventory.append(new_inventory)

    with open('inventory.json', 'w') as json_file:
        json.dump(all_inventory, json_file)


def read_from_AN_response(j_file):
    '''Read a .json format file received after calling Appnexus API
    verify_url service https://api.appnexus.com/url-audit-search
    '''
    verified_urls = []

    with open(j_file, 'r', encoding='utf-8-sig') as myfile:
        data = myfile.read()
        obj = json.loads(data)
        urls = obj['response']['urls']
        # print(urls)
        for u in urls:
            if u.get('found') is True and u.get('audit_status') =='audited':
                url = u['url']
                status = u.get('audit_status')

                verified_urls.append((url, status))

    all_urls = pd.DataFrame(data=verified_urls, columns=['url', 'status'])
    all_urls.to_csv('cross_check_inventory.csv',  index=False)
    # print(verified_urls)
    print ('read_from_AN_response.py finished filtering Xandr API response')


def inventory_by_keywords(dataframe, search_words):
    '''Filter the given df by a search word, return a new subset and
    write the result to a .csv file
    '''

    '''category_list = []
    domains = []
    for index, row in dataframe.iterrows():
        domain = row['Domain']
        level2 = row['Second Level Category']
        top_level= row['Top Level Category']
        plat_imp = row['Platform Audited Imps']
        seller_imp = row['Seller Audited Imps']

        if 'golf' in str(domain).lower() and domain not in domains:
            domains.append(domain)
            category_list.append((domain,plat_imp,seller_imp))

    print(category_list)'''

    d = dataframe[dataframe['Title'].str.contains('|'.join(search_words), case=False, na=False) |
                  dataframe['Description'].str.contains('|'.join(search_words), case=False, na=False) |
                  dataframe['words'].str.contains('|'.join(search_words), case=False, na=False)]

    filtered_inventory = pd.DataFrame(data=d,
                            columns=['sd', 'Country1', 'Country2']).drop_duplicates()
    
    # remove domain id from the domain column
    #category['Domain'] = category['Domain'].str.replace(r'[^aA-zZ.]+', '')
    print('Dataframe contains {0} websites.'.format (len(filtered_inventory)))

    filtered_inventory.to_csv('found_inventory.csv', index=False)


def inventory_by_subcategories(df1, categories_list):
    '''Filter Xandr inventory by the defined subcategories
    '''

    df1['Domain'] = df1['Domain'].str.replace(r'[^aA-zZ.]+', '')

    df2 = df1[(df1['Domain'].str.contains('|'.join(['\.de', '\.com', '\.net', '\.eu']))) &
              (df1['Second Level Category'].str.contains('|'.join(categories_list)))]

    df_filtered = df2[(df2['Domain'].str.contains('Undisclosed') == False)].sort_values(by=['Domain'])

    aggregate = pd.DataFrame(data=df_filtered, columns=['Domain']).drop_duplicates()

    aggregate.to_csv('filtered_inventory.csv', index=False)

    print('Inventory filtered by subcategories. Total domains found: {0}'.format(len(aggregate)))

    return aggregate


def remove_active_users(dataframe):
    '''CSUP 1532 we have one list with active M2O customers
    and a list of all users registered via Hubspot.
    Remove active users of the 1st list from the Hubspot list
    '''
    contacts = pd.DataFrame(data=dataframe, columns=['Contact info'])
    # contacts.to_csv('found_email.csv', index=False, header=False)

    hubspot_data = pd.read_csv('list_to_remove_active_users.csv')
    hb_dataframe = pd.DataFrame(data=hubspot_data)

    condition = hb_dataframe['Email'].isin(contacts['Contact info']) == True 
    hb_dataframe.drop(hb_dataframe[condition].index, inplace = True)
    hb_dataframe.to_csv('cleaned.csv', index=False)
    print(len(hb_dataframe))


def verify_site_domain_perform_and_category_inventory(dataframe):
    ''' Verify whether domains from site_domain_performance report
    can be found on inventory list. This will help to create blacklist
    for domains we do not want to deliver to.
    '''

    # remove AN id from domain inventory list
    dataframe['Domain'] = dataframe['Domain'].str.replace(r'[^aA-zZ.]+', '')

    
    site_perform =  pd.read_csv(
        'financial_sweden/site_domain_performance_30_days.csv')
    site_dataframe = pd.DataFrame(data=site_perform)
    # print(site_dataframe)

    # merge df by inner join, this will filter inventory and performance lists
    df_filtered = pd.merge(
        left=dataframe, right=site_dataframe, how='inner', left_on='Domain',
        right_on='Site Domain')

    # The operators are: | for or, & for and, and ~ for not. These must be
    # grouped by using parentheses.
    result = df_filtered[(
        df_filtered['Domain'].str.contains('Undisclosed') == False)
                             & ((df_filtered['Platform Audited Imps'] > 1000)
                              | (df_filtered['Seller Audited Imps'] > 1000))
                         & (df_filtered['Imps'] > 1000)]
    #save separete columns
    #result = df_filtered[['Seller', 'Domain']]

    result.to_csv('financial_sweden/exported.csv', index=False)

    print(result)
    


def main():
    file = 'WL_SPR.csv'
    j_file = 'AN_verified_urls.json'
    # cc.extract_url('verify.csv')
    data = create_df(file)
    search_words = ['Addiction', 'Treatment','Drug', 'Addiction Treatment',
                    'alcohol', 'opiod', 'opiate', 'cocaine addiction',
                    'holistic addiction', 'types of treatment',
                    'painkiller addiction', 'treatment center', 'rehab',
                    'addcited child', 'therapeutic community', 'impatient',
                    'therapeutic community', 'therapeutic communities',
                    'prescription drug', 'oxycodone', 'methadone', 'marijuana']
    
    search_subcategories = ['Commercial Vehicles','Vehicle Maintenance',
                            'Agriculture & Forestry','Automotive Industry',
                            'Business Operations', 'Property Management',
                            'Engineering & Technology','Auctions']
    
    #inventory_by_subcategories(data, search_subcategories)
    #inventory_by_keywords(data, search_words)
    #read_inventory_file('urls.csv')
    read_from_AN_response(j_file)
    # remove_active_users(data)
    # verify_site_domain_perform_and_category_inventory(data)


if __name__ == '__main__':
    main()
