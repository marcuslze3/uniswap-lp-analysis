import requests
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as tick
from requests.structures import CaseInsensitiveDict
import requests
import json
import warnings
from tabulate import tabulate
warnings.filterwarnings("ignore")

UNIV3_API = "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2"
WETH_USDC_ID = "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"
LAST_TS = 1633059463  # unix for 01/10/2021
APR_1ST_2022 = 1648742399
AUG_1ST_2022 = 1659325063


def client(api_url):
    """
    Initializes a connection to subgraph
    :param api_url: subgraph url for connection
    is_secure: do we need to use https
    :return:
    """
    sample_transport = RequestsHTTPTransport(
        url=api_url,
        verify=True,
        retries=10,
    )
    client = Client(
        transport=sample_transport,
        fetch_schema_from_transport=True
    )
    return client

# CURRENTLY, EACH DATAPOINT IS TREATED AS A SWAP.
# SHOULD TRY TO AGGREGATE DATA TO BE IN FIXED TIME 'BINS'


class UniV2:

    def getData(self, poolID, startTime, endTime):

        self.client = client(UNIV3_API)
        self.data = pd.DataFrame()

        query = ''' query($pair_id: String!,$last_ts:BigInt!){
            swaps(
                first: 10,
                where: {pair: $pair_id,timestamp_gt:$last_ts}, 
                orderBy: timestamp,
                orderDirection: asc
            ) {
                    id
                    amountUSD
                    timestamp
                    amount0In
                    amount0Out
                    amount1In
                    amount1Out
                    pair {
                        id
                    }
                }
            }'''

        cli = self.client
        pair_id = poolID
        last_ts = startTime

        count = 0
        while(count < 2):
            # while(int(last_ts) < endTime):
            params = {
                "pair_id": pair_id,
                "last_ts": last_ts,
            }

            res = cli.execute(gql(query), variable_values=params)
            data = pd.json_normalize(res["swaps"])
            data.rename(columns={'pair.id': 'poolID'}, inplace=True)
            data["price"] = (data["amount0In"].astype(float) + data["amount0Out"].astype(float)) / \
                (data["amount1In"].astype(float) +
                 data["amount1Out"].astype(float))

            self.data = pd.concat([self.data, data], ignore_index=True)

            # grab last timestamp
            last_ts = data.iloc[-1]['timestamp']
            count += 1

        self.data['timestamp'] = pd.to_datetime(
            self.data['timestamp'], unit='s')

        # resampling seems to only work on 1 column, try to do with many. WORST case, resample each amountIn/Out and append?
        print(self.data)
        self.data = self.data.resample('1min', on="timestamp").last()
        #self.data.resample('M', on='timestamp').mean()
        print(self.data)

        # write data to csv so we can access easily in the future
        # self.data.to_csv('data.csv')

        """
        r = requests.post(UNIV3_API, json={'query': query})

        print(r.status_code)
        # print(r.text)
        json_data = json.loads(r.text)
        # print(type(json_data))
        data = pd.json_normalize(json_data["data"]["swaps"])
        data.rename(columns={'pair.id': 'poolID'}, inplace=True)
        #print(tabulate(data, headers='keys', tablefmt='psql'))

        # drop non WETHUSDC rows
        data = data[data.poolID == poolID] 

        # define price as amount0/amount1 in a swap
        print(data["amount0In"].astype(float))
        data["price"] = (data["amount0In"].astype(float) + data["amount0Out"].astype(float)) / \
            (data["amount1In"].astype(float) +
             data["amount1Out"].astype(float))

        print(tabulate(data, headers='keys', tablefmt='psql'))
        self.data = data"""

    def calculateZscore(self, col, window):
        # Compute rolling zscore for column =col and window=window
        col_mean = self.data[col].rolling(window=window).mean()
        col_std = self.data[col].rolling(window=window).std()

        self.data["COL_ZSCORE"] = (self.data[col] - col_mean)/col_std

        print(self.data[col])
        print(col_mean)
        print(col_std)

        print(tabulate(self.data, headers='keys', tablefmt='psql'))


uniV2 = UniV2()

# run this to get data and save into a CSV called data.csv
uniV2.getData(WETH_USDC_ID, LAST_TS, APR_1ST_2022)
# print(uniV2.data)

#uniV2.calculateZscore('price', 7)
