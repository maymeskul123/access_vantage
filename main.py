import requests
import pandas as pd
import datetime, time


# def get_ticket():
symbols = ['ALI.DEX', 'BABA', 'NVE.FRK', 'ABEA.DEX', 'IBM', 'MSFT', 'APLE', 'NVDA', 'AMD',  'AMZN', 'SEB', 'KINS',
           'BMW.FRK', 'TOYOF', 'MKL', 'FB2A.DEX', 'BFOCX']

def ipo_calendar():
    url = 'https://www.alphavantage.co/query?function=IPO_CALENDAR&apikey=ER49QYN4BVCI9UML'
    r = requests.get(url)
    data = r.json()
    print(data)

def CURRENCY_EXCHANGE_RATE(money1, money2):
    url = f'https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency={money1}&to_currency={money2}'\
                +'&apikey=ER49QYN4BVCI9UML'
    r = requests.get(url)
    data = r.json()
    print(data)

def get_ticket(**param):
    url = 'https://www.alphavantage.co/query?'
    parameters = []
    print('ura!!!', param.get('function'))
    for key, val in param.items():
        #print(key, val)
        parameters.append(key)
        url = url + key+'='+val+'&'
    url = url + 'apikey=ER49QYN4BVCI9UML'
    print(parameters)
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
        save_to_parquet(data, sym)
    #data_log = [process_name, function=param.get('function'), date_time, status]
    data_log = [process_name, param.get('function'), date_time, status]
    return (data_log)

def log_add(data):
    columns = ['Process_name', 'Parameter', 'Date', 'Status']
    df = pd.DataFrame([data], columns=columns)
    df.to_csv(r'access.log', mode='a', header=False, index=False)


def save_to_parquet(data_dict, symbol):
    df = pd.DataFrame.from_dict(data_dict, orient="index")
    name_file = 'data_' + symbol + '.parguet'
    df.to_parquet(name_file, engine='auto', compression='snappy')
    #d = pd.read_parquet(name_file)
    # with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
    #     print(d)


if __name__ == '__main__':
    # url = 'https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords=Berkshire&apikey=ER49QYN4BVCI9UML'
    # r = requests.get(url)
    # data = r.json()
    # print(data)

    get_ticket(function='CURRENCY_EXCHANGE_RATE', from_currency='USD', to_currency='UAH')

    # for sym in symbols:
    #     print(f'Getting {sym} ticket...')
    #     data_log = get_ticket(sym)
    #     log_add(data_log)
    #     if data_log[3] == 'Note':
    #         print('Waiting for 65 sec ...')
    #         time.sleep(65)
    #         print(f'Getting {sym} ticket...')
    #         data_log = get_ticket(sym)
    #         log_add(data_log)
    #     elif data_log[3] == 'Error Message':
    #         print(f'Error Message for {sym}')

    #CURRENCY_EXCHANGE_RATE('USD', 'UAH')




