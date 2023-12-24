import pandas as pd
import sqlite3
from pandasql import sqldf
import pandasql as psql
from sqlalchemy import create_engine
import audience_groups


def create_df(data):
    '''
    Convert csv file into Pandas dataframe
    '''

    provided_data = pd.read_csv(data)
    dataframe = pd.DataFrame(data=provided_data)

    return dataframe


def map_dataframes(df1, df2):
    '''
    Bind two sets queried in clickhouse and postgres
    Clickhouse provides all data for each ad.
    Postgres provides filename, an_id for each ad
    '''

    # remove AN id from domain
    # df2['url'] = df2['url'].str.replace(r'[^aA-zZ.]+', '')
    # df1['ip_address'] = df1['ip_address'].str.extract(r'^([0-9]+\.[0-9]+\.[0-9]+)\.[0-9]+$')
    # df2['customer_ip'] = df2['customer_ip'].str.extract(r'^([0-9]+\.[0-9]+\.[0-9]+)\.[0-9]+$')
    
    df1['domain']=df1['domain'].astype(str) #convert order_name variable in df2 to integer
    df2['domain']=df2['domain'].astype(str) #convert order_name variable in df2 to integer
    aggregate = pd.merge(
        left=df1, right=df2, how='inner', left_on=['domain'],
        right_on=['domain']).drop_duplicates()  # sort_values(by=['conv'], ascending = False, kind='quicksort')

    # aggregate = aggregate[['month','region', 'spend']].groupby(['month','region'], as_index=False).sum()
    #aggregate = aggregate.groupby([ 'file_name', 'id'], as_index=False).sum()

    # ***********
    # to take data from website dictionary WL_SPR - domain_rank_dictionary.csv
    # aggregate['duration_minutes'] = round(aggregate['duration_sec']/60, 2)
    # aggregate = aggregate[['site_domain', 'Alexa_rank', 'bounce_rate', 'page_view', 'Daily_Visitors']]
    # ***********

    #****** pandas sum or average rows based on conditions**********
    #regions = ['Varmlands Lan', 'Gavleborgs Lan','Vasternorrlands Lan', 'Stockholms Lan', 'Ostergotlands Lan']
    #rslt_df = aggregate[aggregate['region'].isin(regions)]
    #rslt_df =rslt_df[['region', 'spend']].groupby(['region'], as_index=False).mean()
    #print (rslt_df)
    #************

    aggregate.to_csv('aggregated_data.csv', index=False)

    print('Dataframes joined. Total number of rows: {0}'.
          format(len(aggregate)))
    print('Dataframe has the following columns: {0}'.
          format(aggregate.columns.values))
    return aggregate


def exclude_repeating_rows(df1, df2):
    '''
    create single dataframe excluding vaues from df2
    useful when deleting blacklisted domains from current whitelist
    '''

    # remove AN id from domain
    df2['domain'] = df2['domain'].str.replace(r'[^aA-zZ.]+', '')

    # remove values from df2 in joined df
    df_cleaned = df1.merge(
        df2.drop_duplicates(), on=['domain'],
        how='right',
        indicator=True).query('_merge=="right_only"').drop('_merge', axis=1)  # take not repeating values

    df_cleaned.to_csv('ad_stats.csv', index=False)

    print('Dataframes joined. Total number of rows: {0}'.
          format(len(df_cleaned)))
    print('Dataframe has the following columns: {0}'.
          format(df_cleaned.columns.values))
    return df_cleaned


def audience_report(df1, df2):
    '''
    AUDIENCE REPORT. OPTIONAL
    passing a list of site domains with their audience description for an
    audience report
    '''

    result = pd.merge(
        left=df1, right=df2, how='left', left_on='site_domain',
        right_on='site_domain')

    # df = result.loc[:,~result.columns.duplicated()] removes duplicate columns

    # aggregate = result[['description', 'cost']].groupby(['description'], as_index=False).sum()
    aggregate = result[['description', 'conv', 'cost',]].groupby(
        ['description'], as_index=False).sum().round(2)
   
    
    final_stats = do_sql(aggregate)

    final_stats.to_csv('audience_report.csv', index=False)
    print('Dataframes joined. Total number of rows: {0}'.
          format(len(final_stats)))
    print(final_stats)
    return final_stats


def do_sql(data):
    '''
    Run SQL queries on the merged dataframe
    '''

    # TODO
    engine = create_engine('sqlite://', echo=False)
    data.to_sql('clickhouse_psql', con=engine, index=False)

##    q1 = ("SELECT content_type as content_type, count(content_type) as total" +
##          "FROM clickhouse_psql GROUP BY content_type HAVING total > 0")

    q1 = ("select description, conv, cost, (cost/conv) as cpa " +
          
          "from clickhouse_psql" )

    df = pd.DataFrame(engine.execute(q1).fetchall(),
                      columns=[ "Description", "Conv", "Cost", "CPA"])  # data.columns.values)
    # print(df)
    # df.to_csv('audience_report.csv', index=False)

    return df


def map_xandr_3rdparty(file1, file2):
    '''
    Map 3rd-party data with pixel feed.
    '''

    df1 = create_df(file1)
    df2 = create_df(file2)

    aggregate = pd.merge(
        left=df1, right=df2, how='inner', left_on=['date'],
        right_on=['CET']).drop_duplicates().dropna()

    aggregate.to_csv('aggregated_data.csv', index=False)

    print('Dataframes joined. Total number of rows: {0}'.
          format(len(aggregate)))
    print('Dataframe has the following columns: {0}'.
          format(aggregate.columns.values))


def main():
    folder = 'audience'
    file_click = '/Users/maksym/Desktop/python/{0}/clickhouse.csv'.format(folder)
    #print(file_click)
    file_psql = '/Users/maksym/Desktop/python/{0}/postgres.csv'.format(folder)
    #print(file_psql)
    third_party_data = '/Users/maksym/Desktop/client.csv'
    xandr_pixel_feed = '/Users/maksym/Desktop/xandr.csv'
    # file_top_lvl_cat_id = '/Users/maksym/Desktop/python/{0}/AN_ctegory_id.csv'.format(folder)

    data_click = create_df(file_click)
    #print(data_click)
    data_psql = create_df(file_psql)
    # xandr_categories = create_df(file_top_lvl_cat_id)

    audience_report(data_click, data_psql)
    # map_dataframes(data_click, data_psql)
    # exclude_repeating_rows(data_click, data_psql)
    # merged_tables = audience_report(data_click, data_psql)
    # map_xandr_3rdparty(third_party_data, xandr_pixel_feed)

    # do_sql(merged_tables)

    # print(data_psql.head(10))


if __name__ == '__main__':
    main()
