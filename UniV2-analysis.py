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
UNI_V2_FEE = 0.003  # 0.3% fixed fee


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

    def getSwapData(self, poolID, startTime, endTime):

        self.client = client(UNIV3_API)
        self.swapData = pd.DataFrame()

        query = ''' query($pair_id: String!,$last_ts:BigInt!){
            swaps(
                first: 1000,
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
        # while(count < 2):
        while(int(last_ts) < endTime):
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

            self.swapData = pd.concat([self.swapData, data], ignore_index=True)

            # grab last timestamp
            last_ts = data.iloc[-1]['timestamp']
            count += 1

        self.swapData['timestamp'] = pd.to_datetime(
            self.swapData['timestamp'], unit='s')

        # resampling seems to only work on 1 column, try to do with many. WORST case, resample each amountIn/Out and append?
        print(self.swapData)
        self.swapData = self.swapData.resample('60min', on="timestamp").last()
        self.swapData['timestamp'] = self.swapData.index
        self.swapData['timestamp'] = self.swapData['timestamp'].astype(
            np.int64) // 10**9
        self.swapData.index = range(len(self.swapData))

        self.hourlyPrice = self.swapData[['timestamp', 'price']]

    def getPairHourData(self, poolID, startTime, endTime):

        self.client = client(UNIV3_API)
        self.pairHourData = pd.DataFrame()

        query = ''' query($pair_id: String!,$start_ts:Int!){
            pairHourDatas(
                where: {pair: $pair_id,hourStartUnix_gte:$start_ts}
                orderBy: hourStartUnix
                orderDirection: asc
                first: 100
            ) {
                reserveUSD
                id
                reserve0
                reserve1
                hourStartUnix
                hourlyVolumeToken0
                hourlyVolumeToken1
                hourlyVolumeUSD
            }
        }'''

        cli = self.client
        pair_id = poolID
        # might have to shift hours down by one, or start v last ts mismatch
        start_ts = startTime

        count = 0
        # while(count < 1):
        while(int(start_ts) < endTime):
            params = {
                "pair_id": pair_id,
                "start_ts": start_ts,
            }

            res = cli.execute(gql(query), variable_values=params)
            data = pd.json_normalize(res["pairHourDatas"])
            # print(data)

            # grab last timestamp
            last_ts = data.iloc[-1]['hourStartUnix']
            self.pairHourData = pd.concat(
                [self.pairHourData, data], ignore_index=True)
            count += 1

        self.hourlyVolume = self.pairHourData[['hourStartUnix',
                                              'hourlyVolumeUSD']]

        self.hourlyVolume.rename(
            columns={'hourStartUnix': 'timestamp'}, inplace=True)

    def combineData(self):
        self.combinedData = pd.merge(
            self.hourlyPrice, self.hourlyVolume, on='timestamp')

        # add in our Y variable: fees/TVL
        print(type(self.combinedData['hourlyVolumeUSD'][0]))
        self.combinedData['hourlyFees'] = self.combinedData['hourlyVolumeUSD'].astype(
            float) * UNI_V2_FEE

    def calculateZscore(self, col, window):
        # Compute rolling zscore for column =col and window=window
        col_mean = self.combinedData[col].rolling(window=window).mean()
        col_std = self.combinedData[col].rolling(window=window).std()

        self.combinedData["PRICE_ZSCORE"] = (
            self.combinedData[col] - col_mean)/col_std

        print(self.combinedData[col])
        print(col_mean)
        print(col_std)

        print(tabulate(self.combinedData, headers='keys', tablefmt='psql'))

    def saveData(self):
        # write data to csv so we can access easily in the future
        self.combinedData.to_csv('data.csv')

    def main(self):
        uniV2.getSwapData(WETH_USDC_ID, LAST_TS, AUG_1ST_2022)
        uniV2.getPairHourData(WETH_USDC_ID, LAST_TS, AUG_1ST_2022)
        uniV2.combineData()
        uniV2.calculateZscore('price', 7)
        uniV2.saveData()


uniV2 = UniV2()
uniV2.main()

# run this to get data and save into a CSV called data.csv

"""
print(uniV2.pairHourData)
print(uniV2.swapData)
print(uniV2.hourlyPrice)
print(uniV2.hourlyVolume)
print(uniV2.combinedData)"""

#uniV2.calculateZscore('price', 7)
