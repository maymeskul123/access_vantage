import requests
import pandas as pd
import datetime, time


# def get_ticket():
symbols = ['IBM', 'MSFT', 'APLE', 'NVDA', 'AMD', 'TSCO.LON', 'GPV.TRV', 'DAI.DEX', 'RELIANCE.BSE']

def get_ticket(sym):

    function = 'TIME_SERIES_INTRADAY'
    interval = '5min'
    process_name = f'Getting data from {sym}'
    url = f'https://www.alphavantage.co/query?function={function}&symbol={sym}&interval={interval}&apikey=ER49QYN4BVCI9UML'
    # url = 'https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords=amd&apikey=ER49QYN4BVCI9UML'
    status = 'Successful'
    try:
        r = requests.get(url)
    except requests.exceptions.RequestException as e:
        print(e)
        status = e
    now = datetime.datetime.now()  # current date and time
    date_time = now.strftime("%d/%m/%Y, %H:%M:%S")
    data = r.json()
    print(data)
    err = list(data.keys())
    print(err[0])
    if err[0] == 'Error Message' or err[0] == 'Note':
        status = 'Fail'
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
    for sym in symbols:
        data_log = get_ticket(sym)
        log_add(data_log)
        if data_log[3] == 'Fail':
            print('Waiting for 30 sec ...')
            time.sleep(30)




