import json

import requests
import pandas as pd
import datetime, time

symbols = ['ALI.DEX', 'BABA', 'NVE.FRK', 'ABEA.DEX', 'IBM', 'MSFT', 'APLE', 'NVDA', 'AMD',  'AMZN', 'SEB', 'KINS',
           'BMW.FRK', 'TOYOF', 'MKL', 'FB2A.DEX', 'BFOCX']

functions = [dict(function='TIME_SERIES_INTRADAY', symbol='IBM', interval='1min'),
                dict(function='TIME_SERIES_INTRADAY', symbol='IBM', interval='5min'),
                dict(function='TIME_SERIES_INTRADAY', symbol='IBM', interval='15min'),
                dict(function='TIME_SERIES_INTRADAY', symbol='IBM', interval='30min'),
                dict(function='TIME_SERIES_INTRADAY_EXTENDED', symbol='IBM', interval='1min', slice='year1month1'),
                dict(function='TIME_SERIES_INTRADAY_EXTENDED', symbol='IBM', interval='5min', slice='year1month1'),
                dict(function='TIME_SERIES_INTRADAY_EXTENDED', symbol='IBM', interval='15min', slice='year1month1'),
                dict(function='TIME_SERIES_INTRADAY_EXTENDED', symbol='IBM', interval='30min', slice='year1month1'),
                dict(function='TIME_SERIES_INTRADAY_EXTENDED', symbol='IBM', interval='60min', slice='year1month1'),
                dict(function='CURRENCY_EXCHANGE_RATE', from_currency='USD', to_currency='UAH'),
                dict(function='TIME_SERIES_DAILY', symbol='IBM'),
                dict(function='TIME_SERIES_DAILY_ADJUSTED', symbol='IBM'),
                dict(function='TIME_SERIES_WEEKLY', symbol='IBM'),
                dict(function='TIME_SERIES_WEEKLY_ADJUSTED', symbol='IBM'),
                dict(function='TIME_SERIES_MONTHLY', symbol='IBM'),
                dict(function='TIME_SERIES_MONTHLY_ADJUSTED', symbol='IBM'),
                dict(function='GLOBAL_QUOTE', symbol='IBM'),
                dict(function='SYMBOL_SEARCH', keywords='tesco'),
                dict(function='OVERVIEW', symbol='IBM'),
                dict(function='EARNINGS', symbol='IBM'),
                dict(function='INCOME_STATEMENT', symbol='IBM'),
                dict(function='BALANCE_SHEET', symbol='IBM'),
                dict(function='CASH_FLOW', symbol='IBM'),
                #dict(function='LISTING_STATUS'), #csv - format
                #dict(function='LISTING_STATUS', date='2014-07-10', state='delisted'), #csv - format
                dict(function='CURRENCY_EXCHANGE_RATE', from_currency='USD', to_currency='JPY'),
                dict(function='FX_INTRADAY', from_symbol='EUR', to_symbol='USD', interval='1min'),
                dict(function='FX_INTRADAY', from_symbol='EUR', to_symbol='USD', interval='5min'),
                dict(function='FX_INTRADAY', from_symbol='EUR', to_symbol='USD', interval='15min'),
                dict(function='FX_INTRADAY', from_symbol='EUR', to_symbol='USD', interval='30min'),
                dict(function='FX_INTRADAY', from_symbol='EUR', to_symbol='USD', interval='60min'),
                dict(function='FX_DAILY', from_symbol='EUR', to_symbol='USD'),
                dict(function='FX_DAILY', from_symbol='EUR', to_symbol='USD', outputsize='full'),
                dict(function='FX_WEEKLY', from_symbol='EUR', to_symbol='USD'),
                dict(function='FX_MONTHLY', from_symbol='EUR', to_symbol='USD'),
                dict(function='CURRENCY_EXCHANGE_RATE', from_currency='BTC', to_currency='CNY'),
                dict(function='CURRENCY_EXCHANGE_RATE', from_currency='USD', to_currency='JPY'),
                dict(function='CRYPTO_RATING', symbol='BTC'),
                dict(function='CRYPTO_INTRADAY', symbol='ETH', market='USD', interval='1min'),
                dict(function='CRYPTO_INTRADAY', symbol='ETH', market='USD', interval='5min'),
                dict(function='CRYPTO_INTRADAY', symbol='ETH', market='USD', interval='15min'),
                dict(function='CRYPTO_INTRADAY', symbol='ETH', market='USD', interval='30min'),
                dict(function='CRYPTO_INTRADAY', symbol='ETH', market='USD', interval='60min'),
                dict(function='CRYPTO_INTRADAY', symbol='ETH', market='USD', interval='1min', outputsize='full'),
                dict(function='CRYPTO_INTRADAY', symbol='ETH', market='USD', interval='5min', outputsize='full'),
                dict(function='CRYPTO_INTRADAY', symbol='ETH', market='USD', interval='15min', outputsize='full'),
                dict(function='CRYPTO_INTRADAY', symbol='ETH', market='USD', interval='30min', outputsize='full'),
                dict(function='CRYPTO_INTRADAY', symbol='ETH', market='USD', interval='60min', outputsize='full'),
                dict(function='DIGITAL_CURRENCY_DAILY', symbol='BTC', market='CNY')
             ]

def get_ticket(**param):
    if type(param.get('param')) == dict:
        param = param.get('param')
    url = 'https://www.alphavantage.co/query?'
    parameters = []
    for key, val in param.items():
        #print(key, val)
        parameters.append(key)
        url = url + key+'='+val+'&'
    url = url + 'apikey=ER49QYN4BVCI9UML'
    #print(url)
    process_name = 'AlphaVantage'
    status = 'Successful'
    r = requests.get(url)
    now = datetime.datetime.now()  # current date and time
    date_time = now.strftime("%d/%m/%Y, %H:%M:%S")
    error = False
    try:
        data = r.json()
    except json.decoder.JSONDecodeError as e:
        print(e)
        error = True
    if not error:
        err = list(data.keys())
        if err[0] == 'Error Message' or err[0] == 'Note':
            status = err[0]
        else:
            save_to_parquet(data, param.get('function')+'_' + now.strftime("%d_%m_%Y_%H_%M_%S"))
    else:
        status = 'Fail'
    data_log = [process_name, param, date_time, status]
    log_add(data_log)
    return (data_log)

def log_add(data):
    columns = ['Process_name', 'Parameter', 'Date', 'Status']
    df = pd.DataFrame([data], columns=columns)
    df.to_csv(r'access.log', mode='a', header=False, index=False)


def save_to_parquet(data_dict, name):
    df = pd.DataFrame.from_dict(data_dict, orient="index")
    name_file = name + '.parguet'
    try:
        df.to_parquet(name_file, engine='pyarrow', compression='snappy')
    except ValueError as e:
        print(e)
    #d = pd.read_parquet(name_file)
    # with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
    #     print(d)


if __name__ == '__main__':
    # url = 'https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords=Berkshire&apikey=ER49QYN4BVCI9UML'
    # r = requests.get(url)
    # data = r.json()
    # print(data)

    #get_ticket(param=func1) #Example for call function with dict() in the parameters!!!

    # get_ticket(function='CURRENCY_EXCHANGE_RATE', from_currency='USD', to_currency='UAH')
    #get_ticket(function='TIME_SERIES_DAILY', symbol='IBM')
    #func1 = {'function':'TIME_SERIES_INTRADAY_EXTENDED', 'symbol':'IBM', 'interval':'15min', 'slice':'year1month1'}
    #get_ticket(function='TIME_SERIES_INTRADAY_EXTENDED', symbol='IBM', interval='15min', slice='year1month1')

    # for sym in symbols:
    #     print(f'Getting {sym} ticket...')
    #     data_log = get_ticket(function='TIME_SERIES_INTRADAY', symbol=sym, interval='5min')
    #     if data_log[3] == 'Note':
    #         print('Waiting for 65 sec ...')
    #         time.sleep(65)
    #         print(f'Getting {sym} ticket...')
    #         data_log = get_ticket(function='TIME_SERIES_INTRADAY', symbol=sym, interval='5min')    #
    #     elif data_log[3] == 'Error Message':
    #         print(f'Error Message for {sym}')

    for i, func in enumerate(functions):
        print(f'Getting func{i} {func.get("function")} ticket...')
        data_log = get_ticket(param=func)
        if data_log[3] == 'Note':
            print('Waiting for 65 sec ...')
            time.sleep(65)
            print(f'Getting func{i} {func.get("function")} ticket...')
            data_log = get_ticket(param=func)
        elif data_log[3] == 'Error Message':
            print(f'Error Message for func{i} {func.get("function")}')