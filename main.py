import requests
import pandas as pd
import datetime, time

symbols = ['ALI.DEX', 'BABA', 'NVE.FRK', 'ABEA.DEX', 'IBM', 'MSFT', 'APLE', 'NVDA', 'AMD',  'AMZN', 'SEB', 'KINS',
           'BMW.FRK', 'TOYOF', 'MKL', 'FB2A.DEX', 'BFOCX']

def get_ticket(**param):
    url = 'https://www.alphavantage.co/query?'
    parameters = []
    for key, val in param.items():
        #print(key, val)
        parameters.append(key)
        url = url + key+'='+val+'&'
    url = url + 'apikey=ER49QYN4BVCI9UML'
    print(url)
    process_name = 'AlphaVantage'
    status = 'Successful'
    r = requests.get(url)
    now = datetime.datetime.now()  # current date and time
    date_time = now.strftime("%d/%m/%Y, %H:%M:%S")
    data = r.json()
    print(data)
    err = list(data.keys())
    print(err[0])
    if err[0] == 'Error Message' or err[0] == 'Note':
        status = err[0]
    else:
        save_to_parquet(data, param.get('function')+'_' + now.strftime("%d_%m_%Y_%H_%M_%S"))
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

    get_ticket(function='CURRENCY_EXCHANGE_RATE', from_currency='USD', to_currency='UAH')
    get_ticket(function='TIME_SERIES_DAILY', symbol='IBM')

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