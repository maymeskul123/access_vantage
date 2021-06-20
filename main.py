import requests
import pandas as pd
import datetime, time


# def get_ticket():
symbols = ['ALI.DEX', 'BABA', 'NVE.FRK', 'ABEA.DEX', 'IBM', 'MSFT', 'APLE', 'NVDA', 'AMD',  'AMZN', 'SEB', 'KINS',
           'BMW.FRK', 'TOYOF', 'MKL', 'FB2A.DEX', 'BFOCX']

def get_ticket(sym):

    function = 'TIME_SERIES_INTRADAY'
    interval = '5min'
    process_name = f'Getting data from {sym}'
    url = f'https://www.alphavantage.co/query?function={function}&symbol={sym}&interval={interval}&apikey=ER49QYN4BVCI9UML'
    # url = 'https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords=amd&apikey=ER49QYN4BVCI9UML'
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
    data_log = [process_name, {'function': function, 'interval': interval}, date_time, status]
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

    for sym in symbols:
        print(f'Getting {sym} ticket...')
        data_log = get_ticket(sym)
        log_add(data_log)
        if data_log[3] == 'Note':
            print('Waiting for 65 sec ...')
            time.sleep(65)
            print(f'Getting {sym} ticket...')
            data_log = get_ticket(sym)
            log_add(data_log)
        elif data_log[3] == 'Error Message':
            print(f'Error Message for {sym}')




