# from other_methods import Others_methods
from config import Config

import numpy as np
import pandas as pd
import scipy as sp
import scipy.stats as sps
import scipy.optimize as spop
import yfinance as yf
from math import ceil
from numba import prange

import concurrent.futures
from urllib.parse import urlparse, urlencode
from urllib.request import Request, urlopen
from collections import Counter
import time
from threading import Timer
from _thread import start_new_thread
from statistics import mean
import matplotlib.pyplot as plt
from binance.client import Client
import traceback
from IPython.display import display, HTML, clear_output
from scipy.signal import argrelextrema
import hmac, hashlib, urllib, requests, time, re, json, ssl, statistics, asyncio
import functions, aiomoex, aiohttp, os
from numpy import array as np_array, less, greater
from datetime import datetime, timezone, timedelta
from pandas import DataFrame
import math
# from mplfinance.original_flavor import candlestick_ohlc
import matplotlib.dates as mpl_dates

import warnings
warnings.filterwarnings("ignore")


class Config:
    ClientIds = ('',)  # Торговые счёта
    AccessToken = ''  # Торговый токен доступа ()

    timeframes = {'M1': 1, 'M10': 10, 'H1': 60, 'D1': 24, 'W1': 7, 'm31': 31, 'Q1': 4}
    timeframes_list = [i for i in timeframes]
    training_NN = {"SBER", "VTBR"}  # тикеры по которым обучаем нейросеть
    portfolio = {"SBER", "VTBR"}  # тикеры по которым торгуем и скачиваем исторические данные
    security_board = "TQBR"  # класс тикеров

    # доступные M1, M10, H1
    timeframe_0 = "M1"  # таймфрейм для обучения нейросети - вход и на этом же таймфрейме будем торговать
    timeframe_1 = "M10"  # таймфрейм для обучения нейросети - выход
    timeframe_2 = "H1"  # таймфрейм для обучения нейросети - выход
    timeframe_3 = "D1"
    timeframe_4 = "W1"
    timeframe_5 = "m31"
    timeframe_6 = "Q1"
    start = "2019-01-01"  # с какой даты загружаем исторические данные с MOEX

    trading_hours_start = "10:00"  # время работы биржи - начало
    trading_hours_end = "23:50"  # время работы биржи - конец


async def TimeNow():
    now = datetime.now()
    return now.strftime('%Y-%m-%d %H:%M:%S')
    await asyncio.sleep(0.3)

async def time_stamp_to_normal(inputTime):
    return datetime.utcfromtimestamp(inputTime / 1000).strftime('%Y-%m-%d %H:%M:%S')

async def wait_time(delta_time):
    a = True
    while a:
        await asyncio.sleep(0.1)
        _timenow_ = str(TimeNow()).split(":")
        _timenow_ = _timenow_[len(_timenow_) - 1]
        if int(_timenow_) % 5 == 0:
            a = False

async def TimeStampNow():
    return time.time()

async def check(a, b):
    for i in a:
        if b == i:
            return True
    return False

async def MinetsTimestamp(minets):
    return minets * 60 * 1000

async def TimestampToNormal(inputTime):
    return datetime.utcfromtimestamp(inputTime / 1000).strftime('%Y-%m-%d %H:%M:%S')

async def NormalToTimestamp(inputTime):
    return int(datetime.strptime(inputTime, '%Y-%m-%d %H:%M:%S').timestamp() * 1000)


async def yf_download(coin, start, end, interval):
    df = yf.download(coin, start=start, end=end, interval=interval)['Close']
    # print(df)
    df = df.to_list()
    df = pd.DataFrame({coin: df})
    return df
    await asyncio.sleep(0.3)

async def out_red(text):
    print("\033[31m {}".format(text))
    await asyncio.sleep(0.3)
async def out_green(text):
    print("\033[32m {}".format(text))
    await asyncio.sleep(0.3)
async def out_yellow(text):
    print("\033[33m {}".format(text))
    await asyncio.sleep(0.3)
async def out_blue(text):
    # print(1234)
    print("\033[34m {}".format(text))
    await asyncio.sleep(0.3)
async def out_black(text):
    print("\033[30m {}".format(text))
    await asyncio.sleep(0.3)
async def out_turquoise(text):
    print("\033[36m {}".format(text))
    await asyncio.sleep(0.3)

# async def exception_err(df_error):
#     print(traceback.format_exc())
#     df_error.loc[-1] = [datetime.now().strftime('%Y-%m-%d %H:%M:%S'), traceback.format_exc()]
#     df_error.index = df_error.index + 1
#     df_error = df_error.sort_index()
#     df_error.to_csv(r'df_error.csv', index=False)
#     await asyncio.sleep(0.3)
async def profit_calculate(reduced, deductible, profit, lots, taker, maker):
    profit = (float(reduced) * lots - float(deductible) * lots + float(profit)) - (
                float(reduced) * taker * lots + float(deductible) * maker * lots)
    return profit
    await asyncio.sleep(0.3)

async def profit_calculate_for_cycle(reduced, deductible, lots, taker, maker):
    profit = (float(reduced) * lots - float(deductible) * lots) - (float(reduced) * taker * lots + float(deductible) * maker * lots)
    return profit
    await asyncio.sleep(0.3)

async def transformation(train_part, second_part, third_part):
    # print(f'train_part: {train_part}, second_part: {second_part}, third_part: {third_part}')
    coint = train_part[train_part.columns[0]].tolist()
    coint.append(second_part)
    coint.append(third_part)
    coint = pd.Series(coint)
    return coint
    await asyncio.sleep(0.3)


async def get_candles(session, ticker, timeframe, start, end):
    """Функция получения свечей с MOEX."""
    # print(ticker)
    interval = '1d'
    df1 = yf.download(f'{ticker}.ME', start=start, end=end, interval=interval)
    # print(len(df1))
    last = str(list(df1.index.values)[-1])[:10]
    df = await aiomoex.get_market_candles(session, ticker, interval=timeframe, start=last,          end=end)
    # print(len(df))
    df = pd.DataFrame(df)
    df['datetime'] = pd.to_datetime(df['begin'], format='%Y-%m-%d %H:%M:%S')
    df = df[["datetime", "open", "high", "low", "close", "volume"]].copy()
    df = df.set_index('datetime')
    df = df[last:]
    df = df['close']
    df.drop([last], inplace=True)
    df1 = df1['Close']
    df = df.to_list()
    # print(len(df))
    df = pd.DataFrame({ticker: df})
    df1 = df1.to_list()
    df1 = pd.DataFrame({ticker: df1})
    # result = df1.append(df)
    result = pd.concat([df1, df], axis=0, join='inner')
    result = result.reset_index(drop=True)
    result.to_csv(f"{ticker}.ME.csv")
    # print(result)
    return result


async def get_all_historical_candles(instrument, timeframe, start, end):
    """Запуск асинхронной задачи получения исторических данных для каждого тикера из портфеля."""
    async with aiohttp.ClientSession() as session:
        strategy_tasks = []
        strategy_tasks.append(asyncio.create_task(get_candles(session, instrument, timeframe, start, end)))
        await asyncio.gather(*strategy_tasks)
        await asyncio.sleep(0.3)


async def strategy_one(trade_data, trade_deal,  df_error,  new_bd,  type_active, start, end, interval, stock_1, stock_2):
    # print(stock_1, stock_2)
    is_position_first, is_position_second = False, False
    maker = 0.00035  # 0.000162
    taker = 0.00035  # 0.000324
    final_sum = 20000
    profit = 0
    new_data = -1
    final_sum_begin = final_sum
    i = 0
    if type_active != 'moex':
        df1 = await yf_download(coin=f'{stock_1}', start=start, end=end, interval=interval)
        df2 = await yf_download(coin=f'{stock_2}', start=start, end=end, interval=interval)
    else:
        df1 = await get_all_historical_candles(instrument=f'{stock_1}', start=start, end=end, timeframe=interval)
        df1 = pd.read_csv(f'{stock_1}.ME.csv')
        df1.drop([df1.columns[0]], axis=1, inplace=True)
        # print(df1.columns)
        # print(df1)
        df2 = await get_all_historical_candles(instrument=f'{stock_2}', start=start, end=end, timeframe=interval)
        df2 = pd.read_csv(f'{stock_2}.ME.csv')
        df2.drop([df2.columns[0]], axis=1, inplace=True)
        # print(df2.columns)
        # print(df2)

    while True:
        try:
            end = datetime.now().strftime("%Y-%m-%d")
            i += 1
            if int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-8:-6]) == 0 and 10 <= int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-5:-3]) <= 15 and new_data != int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[8:10]):
                if type_active == 'crypto':
                    df1 = await yf_download(coin=f'{stock_1}', start=start, end=end, interval=interval)
                    df2 = await yf_download(coin=f'{stock_2}', start=start, end=end, interval=interval)
                    new_data = int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[8:10])
                    new_bd.loc[len(new_bd.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active,
                                                             'strategy_one', df1.columns[0],
                                                             'yes', df1]
                    new_bd.loc[len(new_bd.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active,
                                                             'strategy_one', df2.columns[0],
                                                             'yes', df2]
                    # print(df1, df2)
                    await out_yellow(f'Обновляем бд для {type_active}')
                    await out_yellow(f'{df1}, {df2}')

            # if datetime.today().strftime('%A') != 'Monday' and datetime.today().strftime('%A') != 'Sunday':
            if int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-8:-6]) == 0 and 10 <= int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-5:-3]) <= 15 and type_active != 'crypto' and new_data != int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[8:10]):
                if type_active == 'usa':
                    df1 = await yf_download(coin=f'{stock_1}', start=start, end=end, interval=interval)
                    df2 = await yf_download(coin=f'{stock_2}', start=start, end=end, interval=interval)
                    new_data = int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[8:10])
                    new_bd.loc[len(new_bd.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active,
                                                             'strategy_one', df1.columns[0],
                                                             'yes', df1]
                    new_bd.loc[len(new_bd.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active,
                                                             'strategy_one', df2.columns[0],
                                                             'yes', df2]
                    await out_yellow(f'Обновляем бд для {type_active}')
                    await out_yellow(f'{df1}, {df2}')
            if int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-8:-6]) == 19 and 0 <= int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-5:-3]) <= 3 and type_active != 'crypto' and new_data != int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[8:10]):
                if type_active == 'moex':
                    df1 = await get_all_historical_candles(instrument=f'{stock_1}', start=start, end=end, timeframe=interval)
                    df1 = pd.read_csv(f'{stock_1}.ME.csv')
                    df1.drop([df1.columns[0]], axis=1, inplace=True)
                    df2 = await get_all_historical_candles(instrument=f'{stock_2}', start=start, end=end, timeframe=interval)
                    df2 = pd.read_csv(f'{stock_2}.ME.csv')
                    df2.drop([df2.columns[0]], axis=1, inplace=True)
                    new_data = int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[8:10])
                    new_bd.loc[len(new_bd.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active,
                                                             'strategy_one', df1.columns[0],
                                                             'yes', df1]
                    new_bd.loc[len(new_bd.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active,
                                                             'strategy_one', df2.columns[0],
                                                             'yes', df2]
                    await out_yellow(f'Обновляем бд для {type_active}')
                    await out_yellow(f'{df1}, {df2}')

            # if int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-8:-6]) == 0 and 5 <= int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-5:-3]) <= 7:
            #     if type_active == 'crypto':
            #         df1 = await yf_download(coin=f'{stock_1}', start=start, end=end, interval=interval)
            #         df2 = await yf_download(coin=f'{stock_2}', start=start, end=end, interval=interval)
            #         await out_yellow(f'Обновляем бд для {type_active}')
            #         await out_yellow(f'{df1}, {df2}')
            #
            # if datetime.today().strftime('%A') != 'Monday' and datetime.today().strftime('%A') != 'Sunday':
            #     if int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-8:-6]) == 0 and 5 <= int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-5:-3]) <= 7:
            #         if type_active == 'usa':
            #             df1 = await yf_download(coin=f'{stock_1}', start=start, end=end, interval=interval)
            #             df2 = await yf_download(coin=f'{stock_2}', start=start, end=end, interval=interval)
            #             await out_yellow(f'Обновляем бд для {type_active}')
            #             await out_yellow(f'{df1}, {df2}')
            #     if int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-8:-6]) == 19 and 0 <= int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-5:-3]) <= 3 and type_active != 'crypto':
            #         if type_active == 'moex':
            #             df1 = await get_all_historical_candles(instrument=f'{stock_1}', start=start, end=end, timeframe=interval)
            #             df1 = pd.read_csv(f'{stock_1}.ME.csv')
            #             df1.drop([df1.columns[0]], axis=1, inplace=True)
            #             df2 = await get_all_historical_candles(instrument=f'{stock_2}', start=start, end=end, timeframe=interval)
            #             df2 = pd.read_csv(f'{stock_2}.ME.csv')
            #             df2.drop([df2.columns[0]], axis=1, inplace=True)
            #             await out_yellow(f'Обновляем бд для {type_active}')
            #             await out_yellow(f'{df1}, {df2}')

            coint = pd.DataFrame()
            coint_4_changes = pd.DataFrame()
            coint[0] = df1
            coint[1] = df2
            # print(len(df1), len(df2))

            data_set_shape = df1.shape[0] - 1

            slope, intercept, rvalue, pvalue, stderr = sps.linregress(coint[1], coint[0])
            alpha = intercept
            beta = slope
            coint_4 = coint[0] - (coint[1] * beta + alpha)
            slope, intercept, rvalue, pvalue, stderr = sps.linregress(np.array(coint_4[:data_set_shape]),
                                                                      np.array(coint_4[1:]) - np.array(
                                                                          coint_4[:data_set_shape]))

            if coint_4.iloc[-1] < 0 and is_position_first == False and is_position_second == True:
                is_position_second = False
                out_price = coint[1][len(coint[1]) - 1]
                profit = await profit_calculate(reduced=out_price, deductible=enter_price, profit=profit, \
                                          lots=lots, taker=taker, maker=maker)
                profit_for_cycle = await profit_calculate_for_cycle(reduced=out_price, deductible=enter_price, \
                                                              lots=lots, taker=taker, maker=maker)
                final_sum += profit_for_cycle
                await out_red(f'Продали акцию_2 по цене {out_price}, заработок со сделки {profit_for_cycle}')
                await out_blue(f'Заработали всего {profit}, итоговая сумма {final_sum}, кол-во лотов {lots}')
                trade_data.loc[len(trade_data.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active, 'strategy_one', df1.columns[0], df2.columns[0],
                                                         str(round(slope / stderr, 4)), final_sum_begin,
                                                         round(final_sum, 4),
                                                         round((final_sum - final_sum_begin) / final_sum_begin * 100, 4)]
                trade_deal.loc[len(trade_data.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active, 'strategy_one', df2.columns[0],
                                                         'sell', out_price]
                print()

            if coint_4.iloc[-1] > 0 and is_position_first == True and is_position_second == False:
                is_position_first = False
                out_price = coint[0][len(coint[0]) - 1]
                profit = await profit_calculate(reduced=out_price, deductible=enter_price, profit=profit, \
                                          lots=lots, taker=taker, maker=maker)
                profit_for_cycle = await profit_calculate_for_cycle(reduced=out_price, deductible=enter_price, \
                                                              lots=lots, taker=taker, maker=maker)
                final_sum += profit_for_cycle
                await out_red(f'Продали акцию_1 по цене {out_price}, заработок со сделки {profit_for_cycle}')
                await out_blue(f'Заработали всего {profit}, итоговая сумма {final_sum}, кол-во лотов {lots}')
                trade_data.loc[len(trade_data.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active, 'strategy_one', df1.columns[0], df2.columns[0],
                                                         str(round(slope / stderr, 4)), final_sum_begin,
                                                         round(final_sum, 4),
                                                         round((final_sum - final_sum_begin) / final_sum_begin * 100, 4)]
                trade_deal.loc[len(trade_data.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active, 'strategy_one', df1.columns[0],
                                                         'sell', out_price]
                print()

            if coint_4.iloc[-1] < 0 and coint_4.iloc[-2] > 0 and is_position_first == False and is_position_second == False:
                lots = final_sum / coint[0][len(coint[0]) - 1]
                enter_price = coint[0][len(coint[0]) - 1]
                is_position_first = True
                await out_green(f'Купили акцию_1 по цене {enter_price}, зашли по coint_4 {coint_4.iloc[-1]}, кол-во лотов {lots}')
                await out_yellow('Т-статистика коинтеграционного теста ' + str(round(slope / stderr, 4)))
                trade_deal.loc[len(trade_data.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active, 'strategy_one', df1.columns[0],
                                                         'buy', enter_price]

            if coint_4.iloc[-1] > 0 and coint_4.iloc[-2] < 0 and is_position_first == False and is_position_second == False:
                lots = final_sum / coint[1][len(coint[1]) - 1]
                enter_price = coint[1][len(coint[1]) - 1]
                is_position_second = True
                await out_green(f'Купили акцию_2 по цене {enter_price}, зашли по coint_4 {coint_4.iloc[-1]}, кол-во лотов {lots}')
                await out_yellow('Т-статистика коинтеграционного теста ' + str(round(slope / stderr, 4)))
                trade_deal.loc[len(trade_data.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active, 'strategy_one', df2.columns[0],
                                                         'buy', enter_price]

            if i % 2000 == 0:
                clear_output()

            # await out_blue(f'Текущее время: {await TimeNow()}, цена монеты один {coint[0][len(coint[0]) - 1]}, цена монеты два {coint[1][len(coint[1]) - 1]}, пара: {df1.columns[0]}-{df2.columns[0]}, стратегия: strategy_one, конечная сумма: {round(final_sum, 4)}, прибыль: {round(final_sum, 4) - final_sum_begin}, в процентах {round((final_sum - final_sum_begin) / final_sum_begin * 100, 4)}, открытали первая позиция {is_position_first}, открытали вторая позиция {is_position_second}')
            await out_blue(f'Текущее время: {await TimeNow()}, цена монеты один {coint[0].iloc[-1]}, цена монеты два {coint[1].iloc[-1]}, пара: {df1.columns[0]}-{df2.columns[0]}, стратегия: strategy_one, конечная сумма: {round(final_sum, 4)}, прибыль: {round(final_sum, 4) - final_sum_begin}, в процентах {round((final_sum - final_sum_begin) / final_sum_begin * 100, 4)}, открыта ли первая позиция {is_position_first}, открыта ли вторая позиция {is_position_second}')
            # await out_blue(f'Пара: {df1.columns[0]}-{df2.columns[0]}, стратегия: strategy_one, конечная сумма: {round(final_sum, 4)}, прибыль: {round(final_sum, 4) - final_sum_begin}, в процентах {round((final_sum - final_sum_begin) / final_sum_begin * 100, 4)}')
            trade_data.to_csv(f'Live_cointegraation_trading.csv', index=False)
            trade_deal.to_csv(f'Live_cointegraation_trading_deals.csv', index=False)
            new_bd.to_csv(f'New_bd.csv', index=False)
            await asyncio.sleep(0.3)

        except Exception as err:
            print(traceback.format_exc())
            df_error.loc[-1] = [datetime.now().strftime('%Y-%m-%d %H:%M:%S'), traceback.format_exc()]
            df_error.index = df_error.index + 1
            df_error = df_error.sort_index()
            df_error.to_csv(r'df_error.csv', index=False)


async def strategy_two(trade_data, trade_deal,  df_error,  new_bd,  type_active, start, end, interval, stock_1, stock_2):
    # print(stock_1, stock_2)
    is_position_first, is_position_second = False, False
    maker = 0.00035  # 0.000162
    taker = 0.00035  # 0.000324
    final_sum = 10000
    profit = 0
    final_sum_begin = final_sum
    i = 0
    new_data = -1
    if type_active != 'moex':
        df1 = await yf_download(coin=f'{stock_1}', start=start, end=end, interval=interval)
        df2 = await yf_download(coin=f'{stock_2}', start=start, end=end, interval=interval)
    else:
        df1 = await get_all_historical_candles(instrument=f'{stock_1}', start=start, end=end, timeframe=interval)
        df1 = pd.read_csv(f'{stock_1}.ME.csv')
        df1.drop([df1.columns[0]], axis=1, inplace=True)

        df2 = await get_all_historical_candles(instrument=f'{stock_2}', start=start, end=end, timeframe=interval)
        df2 = pd.read_csv(f'{stock_2}.ME.csv')
        df2.drop([df2.columns[0]], axis=1, inplace=True)

    while True:
        try:
            end = datetime.now().strftime("%Y-%m-%d")
            # print(int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-8:-6]), int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-5:-3]), datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            if int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-8:-6]) == 0 and 10 <= int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-5:-3]) <= 15 and new_data != int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[8:10]):
                if type_active == 'crypto':
                    df1 = await yf_download(coin=f'{stock_1}', start=start, end=end, interval=interval)
                    df2 = await yf_download(coin=f'{stock_2}', start=start, end=end, interval=interval)
                    new_data = int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[8:10])
                    new_bd.loc[len(new_bd.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active,
                                                             'strategy_two', df1.columns[0],
                                                             'yes', df1]
                    new_bd.loc[len(new_bd.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active,
                                                             'strategy_two', df2.columns[0],
                                                             'yes', df2]
                    # print(df1, df2)
                    await out_yellow(f'Обновляем бд для {type_active}')
                    await out_yellow(f'{df1}, {df2}')

            # if datetime.today().strftime('%A') != 'Monday' and datetime.today().strftime('%A') != 'Sunday':
            if int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-8:-6]) == 0 and 10 <= int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-5:-3]) <= 15 and type_active != 'crypto' and new_data != int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[8:10]):
                if type_active == 'usa':
                    df1 = await yf_download(coin=f'{stock_1}', start=start, end=end, interval=interval)
                    df2 = await yf_download(coin=f'{stock_2}', start=start, end=end, interval=interval)
                    new_data = int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[8:10])
                    new_bd.loc[len(new_bd.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active,
                                                             'strategy_two', df1.columns[0],
                                                             'yes', df1]
                    new_bd.loc[len(new_bd.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active,
                                                             'strategy_two', df2.columns[0],
                                                             'yes', df2]
                    await out_yellow(f'Обновляем бд для {type_active}')
                    await out_yellow(f'{df1}, {df2}')
            if int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-8:-6]) == 19 and 0 <= int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-5:-3]) <= 3 and type_active != 'crypto' and new_data != int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[8:10]):
                if type_active == 'moex':
                    df1 = await get_all_historical_candles(instrument=f'{stock_1}', start=start, end=end, timeframe=interval)
                    df1 = pd.read_csv(f'{stock_1}.ME.csv')
                    df1.drop([df1.columns[0]], axis=1, inplace=True)
                    df2 = await get_all_historical_candles(instrument=f'{stock_2}', start=start, end=end, timeframe=interval)
                    df2 = pd.read_csv(f'{stock_2}.ME.csv')
                    df2.drop([df2.columns[0]], axis=1, inplace=True)
                    new_data = int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[8:10])
                    new_bd.loc[len(new_bd.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active,
                                                             'strategy_two', df1.columns[0],
                                                             'yes', df1]
                    new_bd.loc[len(new_bd.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active,
                                                             'strategy_two', df2.columns[0],
                                                             'yes', df2]
                    await out_yellow(f'Обновляем бд для {type_active}')
                    await out_yellow(f'{df1}, {df2}')



            # if int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-8:-6]) == 0 and 5 <= int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-5:-3]) <= 7:
            #     if type_active == 'crypto':
            #         df1 = await yf_download(coin=f'{stock_1}', start=start, end=end, interval=interval)
            #         df2 = await yf_download(coin=f'{stock_2}', start=start, end=end, interval=interval)
            #         await out_yellow(f'Обновляем бд для {type_active}')
            #         await out_yellow(f'{df1}, {df2}')
            #
            # if datetime.today().strftime('%A') != 'Monday' and datetime.today().strftime('%A') != 'Sunday':
            #     if int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-8:-6]) == 0 and 5 <= int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-5:-3]) <= 7:
            #         if type_active == 'usa':
            #             df1 = await yf_download(coin=f'{stock_1}', start=start, end=end, interval=interval)
            #             df2 = await yf_download(coin=f'{stock_2}', start=start, end=end, interval=interval)
            #             await out_yellow(f'Обновляем бд для {type_active}')
            #             await out_yellow(f'{df1}, {df2}')
            #     if int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-8:-6]) == 19 and 0 <= int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-5:-3]) <= 3 and type_active != 'crypto':
            #         if type_active == 'moex':
            #             df1 = await get_all_historical_candles(instrument=f'{stock_1}', start=start, end=end, timeframe=interval)
            #             df1 = pd.read_csv(f'{stock_1}.ME.csv')
            #             df1.drop([df1.columns[0]], axis=1, inplace=True)
            #             df2 = await get_all_historical_candles(instrument=f'{stock_2}', start=start, end=end, timeframe=interval)
            #             df2 = pd.read_csv(f'{stock_2}.ME.csv')
            #             df2.drop([df2.columns[0]], axis=1, inplace=True)
            #             await out_yellow(f'Обновляем бд для {type_active}')
            #             await out_yellow(f'{df1}, {df2}')

            coint = pd.DataFrame()
            coint_4_changes = pd.DataFrame()
            coint[0] = df1
            coint[1] = df2
            # print(df1)
            # print(df2)
            data_set_shape = df1.shape[0] - 1

            slope, intercept, rvalue, pvalue, stderr = sps.linregress(coint[1], coint[0])
            alpha = intercept
            beta = slope
            coint_4 = coint[0] - (coint[1] * beta + alpha)
            slope, intercept, rvalue, pvalue, stderr = sps.linregress(np.array(coint_4[:data_set_shape]),
                                                                      np.array(coint_4[1:]) - np.array(
                                                                          coint_4[:data_set_shape]))

            if final_sum < 0:
                await out_yellow('Закрылись по стопу')
                break

            if (is_position_first == True or is_position_second == True) and coint_4.iloc[-1] / enter_price_sell >= 2:
                await out_yellow('Закрылись по стопу')
                break

            if coint_4.iloc[-1] < 0 and is_position_first == False and is_position_second == True:
                is_position_second = False
                out_price_sell = coint[1][len(coint[1]) - 1]
                out_price_buy = coint[0][len(coint[0]) - 1]
                profit_1 = await profit_calculate(reduced=out_price_sell, deductible=enter_price_buy, profit=profit, \
                                            lots=lots_buy, taker=taker, maker=maker)
                profit_2 = await profit_calculate(reduced=enter_price_sell, deductible=out_price_buy, profit=profit, \
                                            lots=lots_sell, taker=taker, maker=maker)
                profit_for_cycle_1 = await profit_calculate_for_cycle(reduced=out_price_sell, deductible=enter_price_buy, \
                                                                lots=lots_buy, taker=taker, maker=maker)
                profit_for_cycle_2 = await profit_calculate_for_cycle(reduced=enter_price_sell, deductible=out_price_buy, \
                                                                lots=lots_sell, taker=taker, maker=maker)
                final_sum += profit_for_cycle_1
                final_sum += profit_for_cycle_2
                await out_red(f'Продали акцию_2 по цене {out_price_sell}, заработок со сделки {profit_for_cycle_1}')
                await out_red(f'Купили акцию_1 по цене {out_price_buy}, заработок со сделки {profit_for_cycle_2}')
                await out_blue(f'Заработали всего {profit_1 + profit_2}, итоговая сумма {final_sum}')
                trade_data.loc[len(trade_data.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active, 'strategy_two', df1.columns[0], df2.columns[0],
                                                         str(round(slope / stderr, 4)), final_sum_begin,
                                                         round(final_sum, 4),
                                                         round((final_sum - final_sum_begin) / final_sum_begin * 100, 4)]
                trade_deal.loc[len(trade_data.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active, 'strategy_two', df1.columns[0],
                                                         'buy', out_price_buy]
                trade_deal.loc[len(trade_data.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active, 'strategy_two', df2.columns[0],
                                                         'sell', out_price_sell]
                # print(df1)

            if coint_4.iloc[-1] > 0 and is_position_first == True and is_position_second == False:
                is_position_first = False
                out_price_sell = coint[0][len(coint[0]) - 1]
                out_price_buy = coint[1][len(coint[1]) - 1]
                profit_1 = await profit_calculate(reduced=out_price_sell, deductible=enter_price_buy, profit=profit, \
                                            lots=lots_buy, taker=taker, maker=maker)
                profit_2 = await profit_calculate(reduced=enter_price_sell, deductible=out_price_buy, profit=profit, \
                                            lots=lots_sell, taker=taker, maker=maker)
                profit_for_cycle_1 = await profit_calculate_for_cycle(reduced=out_price_sell, deductible=enter_price_buy, \
                                                                lots=lots_buy, taker=taker, maker=maker)
                profit_for_cycle_2 = await profit_calculate_for_cycle(reduced=enter_price_sell, deductible=out_price_buy, \
                                                                lots=lots_sell, taker=taker, maker=maker)
                final_sum += profit_for_cycle_1
                final_sum += profit_for_cycle_2
                await out_red(f'Продали акцию_1 по цене {out_price_sell}, заработок со сделки {profit_for_cycle_1}')
                await out_red(f'Купили акцию_2 по цене {out_price_buy}, заработок со сделки {profit_for_cycle_2}')
                await out_blue(f'Заработали всего {profit_1 + profit_2}, итоговая сумма {final_sum}')
                trade_data.loc[len(trade_data.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active, 'strategy_two', df1.columns[0], df2.columns[0],
                                                         str(round(slope / stderr, 4)), final_sum_begin,
                                                         round(final_sum, 4),
                                                         round((final_sum - final_sum_begin) / final_sum_begin * 100, 4)]
                trade_deal.loc[len(trade_data.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active, 'strategy_two', df2.columns[0],
                                                         'buy', out_price_buy]
                trade_deal.loc[len(trade_data.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active, 'strategy_two', df1.columns[0],
                                                         'sell', out_price_sell]
                print()

            if coint_4.iloc[-1] < 0 and coint_4.iloc[-2] > 0 and is_position_first == False and is_position_second == False:
                lots_buy = final_sum / coint[0][len(coint[0]) - 1]
                lots_sell = final_sum / coint[1][len(coint[1]) - 1]
                enter_price_buy = coint[0][len(coint[0]) - 1]
                enter_price_sell = coint[1][len(coint[1]) - 1]
                is_position_first = True
                await out_green(f'Купили акцию_1 по цене {enter_price_buy}, зашли по coint_4 {coint_4.iloc[-1]}, кол-во лотов {lots_buy}')
                await out_green(f'Продали акцию_2 по цене {enter_price_sell}, зашли по coint_4 {coint_4.iloc[-1]}, кол-во лотов {lots_sell}')
                trade_deal.loc[len(trade_data.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active, 'strategy_two', df1.columns[0],
                                                         'buy', enter_price_buy]
                trade_deal.loc[len(trade_data.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active, 'strategy_two', df2.columns[0],
                                                         'sell', enter_price_sell]

            if coint_4.iloc[-1] > 0 and coint_4.iloc[-2] < 0 and is_position_first == False and is_position_second == False:
                lots_buy = final_sum / coint[1][len(coint[1]) - 1]
                lots_sell = final_sum / coint[0][len(coint[0]) - 1]
                enter_price_buy = coint[1][len(coint[1]) - 1]
                enter_price_sell = coint[0][len(coint[0]) - 1]
                is_position_second = True
                await out_green(f'Купили акцию_2 по цене {enter_price_buy}, зашли по coint_4 {coint_4.iloc[-1]}, кол-во лотов {lots_buy}')
                await out_green(f'Продали акцию_1 по цене {enter_price_sell}, зашли по coint_4 {coint_4.iloc[-1]}, кол-во лотов {lots_sell}')
                trade_deal.loc[len(trade_data.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active, 'strategy_two', df2.columns[0],
                                                         'buy', enter_price_buy]
                trade_deal.loc[len(trade_data.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active, 'strategy_two', df1.columns[0],
                                                         'sell', enter_price_sell]

            if i % 2000 == 0:
                clear_output()

            # await out_blue(f'Текущее время: {await TimeNow()}, цена монеты один {coint[0][len(coint[0]) - 1]}, цена монеты два {coint[1][len(coint[1]) - 1]}, пара: {df1.columns[0]}-{df2.columns[0]}, стратегия: strategy_two, конечная сумма: {round(final_sum, 4)}, прибыль: {round(final_sum, 4) - final_sum_begin}, в процентах {round((final_sum - final_sum_begin) / final_sum_begin * 100, 4)}, открытали первая позиция {is_position_first}, открытали вторая позиция {is_position_second}')
            await out_blue(f'Текущее время: {await TimeNow()}, цена монеты один {coint[0].iloc[-1]}, цена монеты два {coint[1].iloc[-1]}, пара: {df1.columns[0]}-{df2.columns[0]}, стратегия: strategy_two, конечная сумма: {round(final_sum, 4)}, прибыль: {round(final_sum, 4) - final_sum_begin}, в процентах {round((final_sum - final_sum_begin) / final_sum_begin * 100, 4)}, открыта ли первая позиция {is_position_first}, открыта ли вторая позиция {is_position_second}')
            # print(coint[1])
            # await out_blue(f'Пара: {df1.columns[0]}-{df2.columns[0]}, стратегия: strategy_two, конечная сумма: {round(final_sum, 4)}, прибыль: {round(final_sum, 4) - final_sum_begin}, в процентах {round((final_sum - final_sum_begin) / final_sum_begin * 100, 4)}')
            trade_data.to_csv(f'Live_cointegraation_trading.csv', index=False)
            trade_deal.to_csv(f'Live_cointegraation_trading_deals.csv', index=False)
            new_bd.to_csv(f'New_bd.csv', index=False)
            await asyncio.sleep(0.3)

        except Exception as err:
            print(traceback.format_exc())
            df_error.loc[-1] = [datetime.now().strftime('%Y-%m-%d %H:%M:%S'), traceback.format_exc()]
            df_error.index = df_error.index + 1
            df_error = df_error.sort_index()
            df_error.to_csv(r'df_error.csv', index=False)


async def strategy_three(trade_data, trade_deal,  df_error,  new_bd,  type_active, start, end, interval, stock_1, stock_2):
    # print(stock_1, stock_2)
    is_position_first, is_position_second = False, False
    maker = 0.00035  # 0.000162
    taker = 0.00035  # 0.000324
    final_sum = 10000
    profit = 0
    final_sum_begin = final_sum
    years_test = 2
    trade_period = (int(end[:4]) - int(start[:4]))
    i = 0
    new_data = -1
    if type_active != 'moex':
        df1 = await yf_download(coin=f'{stock_1}', start=start, end=end, interval=interval)
        df2 = await yf_download(coin=f'{stock_2}', start=start, end=end, interval=interval)
    else:
        df1 = await get_all_historical_candles(instrument=f'{stock_1}', start=start, end=end, timeframe=interval)
        df1 = pd.read_csv(f'{stock_1}.ME.csv')
        df1.drop([df1.columns[0]], axis=1, inplace=True)

        df2 = await get_all_historical_candles(instrument=f'{stock_2}', start=start, end=end, timeframe=interval)
        df2 = pd.read_csv(f'{stock_2}.ME.csv')
        df2.drop([df2.columns[0]], axis=1, inplace=True)

    test_part = ceil(len(df1) / trade_period * years_test)
    train_part = len(df2) - test_part

    df1_train = df1[:train_part]
    df2_train = df2[:train_part]

    while True:
        try:
            end = datetime.now().strftime("%Y-%m-%d")
            # print(new_data)
            if int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-8:-6]) == 0 and 10 <= int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-5:-3]) <= 15 and new_data != int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[8:10]):
                if type_active == 'crypto':
                    df1 = await yf_download(coin=f'{stock_1}', start=start, end=end, interval=interval)
                    df2 = await yf_download(coin=f'{stock_2}', start=start, end=end, interval=interval)
                    new_data = int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[8:10])
                    new_bd.loc[len(new_bd.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active,
                                                             'strategy_three', df1.columns[0],
                                                             'yes', df1]
                    new_bd.loc[len(new_bd.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active,
                                                             'strategy_three', df2.columns[0],
                                                             'yes', df2]
                    # print(df1, df2)
                    await out_yellow(f'Обновляем бд для {type_active}')
                    await out_yellow(f'{df1}, {df2}')

            # if datetime.today().strftime('%A') != 'Monday' and datetime.today().strftime('%A') != 'Sunday':
            if int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-8:-6]) == 0 and 10 <= int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-5:-3]) <= 15 and type_active != 'crypto' and new_data != int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[8:10]):
                if type_active == 'usa':
                    df1 = await yf_download(coin=f'{stock_1}', start=start, end=end, interval=interval)
                    df2 = await yf_download(coin=f'{stock_2}', start=start, end=end, interval=interval)
                    new_data = int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[8:10])
                    new_bd.loc[len(new_bd.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active,
                                                             'strategy_three', df1.columns[0],
                                                             'yes', df1]
                    new_bd.loc[len(new_bd.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active,
                                                             'strategy_three', df2.columns[0],
                                                             'yes', df2]
                    await out_yellow(f'Обновляем бд для {type_active}')
                    await out_yellow(f'{df1}, {df2}')
            if int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-8:-6]) == 19 and 0 <= int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-5:-3]) <= 3 and type_active != 'crypto' and new_data != int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[8:10]):
                if type_active == 'moex':
                    df1 = await get_all_historical_candles(instrument=f'{stock_1}', start=start, end=end, timeframe=interval)
                    df1 = pd.read_csv(f'{stock_1}.ME.csv')
                    df1.drop([df1.columns[0]], axis=1, inplace=True)
                    df2 = await get_all_historical_candles(instrument=f'{stock_2}', start=start, end=end, timeframe=interval)
                    df2 = pd.read_csv(f'{stock_2}.ME.csv')
                    df2.drop([df2.columns[0]], axis=1, inplace=True)
                    new_data = int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[8:10])
                    new_bd.loc[len(new_bd.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active,
                                                             'strategy_three', df1.columns[0],
                                                             'yes', df1]
                    new_bd.loc[len(new_bd.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active,
                                                             'strategy_three', df2.columns[0],
                                                             'yes', df2]
                    await out_yellow(f'Обновляем бд для {type_active}')
                    await out_yellow(f'{df1}, {df2}')




            # if int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-8:-6]) == 0 and 5 <= int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-5:-3]) <= 7:
            #     if type_active == 'crypto':
            #         df1 = await yf_download(coin=f'{stock_1}', start=start, end=end, interval=interval)
            #         df2 = await yf_download(coin=f'{stock_2}', start=start, end=end, interval=interval)
            #         await out_yellow(f'Обновляем бд для {type_active}')
            #         await out_yellow(f'{df1}, {df2}')
            #
            # if datetime.today().strftime('%A') != 'Monday' and datetime.today().strftime('%A') != 'Sunday':
            #     if int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-8:-6]) == 0 and 5 <= int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-5:-3]) <= 7:
            #         if type_active == 'usa':
            #             df1 = await yf_download(coin=f'{stock_1}', start=start, end=end, interval=interval)
            #             df2 = await yf_download(coin=f'{stock_2}', start=start, end=end, interval=interval)
            #             await out_yellow(f'Обновляем бд для {type_active}')
            #             await out_yellow(f'{df1}, {df2}')
            #     if int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-8:-6]) == 19 and 0 <= int(datetime.now().strftime('%Y-%m-%d %H:%M:%S')[-5:-3]) <= 3 and type_active != 'crypto':
            #         if type_active == 'moex':
            #             df1 = await get_all_historical_candles(instrument=f'{stock_1}', start=start, end=end, timeframe=interval)
            #             df1 = pd.read_csv(f'{stock_1}.ME.csv')
            #             df1.drop([df1.columns[0]], axis=1, inplace=True)
            #             df2 = await get_all_historical_candles(instrument=f'{stock_2}', start=start, end=end, timeframe=interval)
            #             df2 = pd.read_csv(f'{stock_2}.ME.csv')
            #             df2.drop([df2.columns[0]], axis=1, inplace=True)
            #             await out_yellow(f'Обновляем бд для {type_active}')
            #             await out_yellow(f'{df1}, {df2}')


            coint = pd.DataFrame()
            coint_4_changes = pd.DataFrame()
            coint[0] = df1
            coint[1] = df2

            data_set_shape = df1.shape[0] - 1
            # print(coint[0], coint[1])
            # print(df1_train, df2_train)
            coint_5 = await transformation(df1_train, coint[0][len(coint[0]) - 1], coint[0][len(coint[0]) - 1])
            coint_6 = await transformation(df2_train, coint[1][len(coint[1]) - 1], coint[1][len(coint[1]) - 1])
            # print(coint_6, coint_5)
            slope, intercept, rvalue, pvalue, stderr = sps.linregress(coint_6, coint_5)
            alpha = intercept
            beta = slope
            coint_4 = coint_5 - (coint_6 * beta + alpha)
            a = len(coint_4)
            slope, intercept, rvalue, pvalue, stderr = sps.linregress(np.array(coint_4[:a - 1]),
                                                                      np.array(coint_4[1:]) - np.array(coint_4[:a - 1]))

            if coint_4.iloc[-1] < 0 and is_position_first == False and is_position_second == True:
                is_position_second = False
                out_price = coint[1][len(coint[0]) - 1]
                profit = await profit_calculate(reduced=out_price, deductible=enter_price, profit=profit, \
                                          lots=lots, taker=taker, maker=maker)
                profit_for_cycle = await profit_calculate_for_cycle(reduced=out_price, deductible=enter_price, \
                                                              lots=lots, taker=taker, maker=maker)
                final_sum += profit_for_cycle
                await out_red(f'Продали акцию_2 по цене {out_price}, заработок со сделки {profit_for_cycle}')
                await out_blue(f'Заработали всего {profit}, итоговая сумма {final_sum}, кол-во лотов {lots}')
                trade_data.loc[len(trade_data.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active, 'strategy_three', df1.columns[0], df2.columns[0],
                                                         str(round(slope / stderr, 4)), final_sum_begin,
                                                         round(final_sum, 4),
                                                         round((final_sum - final_sum_begin) / final_sum_begin * 100, 4)]
                trade_deal.loc[len(trade_data.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active, 'strategy_three', df2.columns[0],
                                                         'sell', out_price]
                print()

            if coint_4.iloc[-1] > 0 and is_position_first == True and is_position_second == False:
                is_position_first = False
                out_price = coint[0][len(coint[0]) - 1]
                profit = await profit_calculate(reduced=out_price, deductible=enter_price, profit=profit, \
                                          lots=lots, taker=taker, maker=maker)
                profit_for_cycle = await profit_calculate_for_cycle(reduced=out_price, deductible=enter_price, \
                                                              lots=lots, taker=taker, maker=maker)
                final_sum += profit_for_cycle
                await out_red(f'Продали акцию_1 по цене {out_price}, заработок со сделки {profit_for_cycle}')
                await out_blue(f'Заработали всего {profit}, итоговая сумма {final_sum}, кол-во лотов {lots}')
                trade_data.loc[len(trade_data.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active, 'strategy_three', df1.columns[0], df2.columns[0],
                                                         str(round(slope / stderr, 4)), final_sum_begin,
                                                         round(final_sum, 4),
                                                         round((final_sum - final_sum_begin) / final_sum_begin * 100, 4)]
                trade_deal.loc[len(trade_data.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active, 'strategy_three', df1.columns[0],
                                                         'sell', out_price]
                print()

            if coint_4.iloc[-1] < 0 and coint_4.iloc[-2] > 0 and is_position_first == False and is_position_second == False:
                lots = final_sum / coint[0][len(coint[0]) - 1]
                enter_price = coint[0][len(coint[0]) - 1]
                is_position_first = True
                await out_green(f'Купили акцию_1 по цене {enter_price}, зашли по coint_4 {coint_4.iloc[-1]}, кол-во лотов {lots}')
                await out_yellow('Т-статистика коинтеграционного теста ' + str(round(slope / stderr, 4)))
                trade_deal.loc[len(trade_data.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active, 'strategy_three', df1.columns[0],
                                                         'buy', enter_price]

            if coint_4.iloc[-1] > 0 and coint_4.iloc[-2] < 0 and is_position_first == False and is_position_second == False:
                lots = final_sum / coint[1][len(coint[0]) - 1]
                enter_price = coint[1][len(coint[0]) - 1]
                is_position_second = True
                await out_green(f'Купили акцию_2 по цене {enter_price}, зашли по coint_4 {coint_4.iloc[-1]}, кол-во лотов {lots}')
                await out_yellow('Т-статистика коинтеграционного теста ' + str(round(slope / stderr, 4)))
                trade_deal.loc[len(trade_data.index)] = [datetime.now().strftime("%Y-%m-%d"), type_active, 'strategy_three', df2.columns[0],
                                                         'buy', enter_price]


            if i % 2000 == 0:
                clear_output()

            # await out_blue(f'Текущее время: {await TimeNow()}, цена монеты один {coint[0][len(coint[0]) - 1]}, цена монеты два {coint[1][len(coint[1]) - 1]}, пара: {df1.columns[0]}-{df2.columns[0]}, стратегия: strategy_three, конечная сумма: {round(final_sum, 4)}, прибыль: {round(final_sum, 4) - final_sum_begin}, в процентах {round((final_sum - final_sum_begin) / final_sum_begin * 100, 4)}, открытали первая позиция {is_position_first}, открытали вторая позиция {is_position_second}')
            await out_blue(f'Текущее время: {await TimeNow()}, цена монеты один {coint[0].iloc[-1]}, цена монеты два {coint[1].iloc[-1]}, пара: {df1.columns[0]}-{df2.columns[0]}, стратегия: strategy_three, конечная сумма: {round(final_sum, 4)}, прибыль: {round(final_sum, 4) - final_sum_begin}, в процентах {round((final_sum - final_sum_begin) / final_sum_begin * 100, 4)}, открыта ли первая позиция {is_position_first}, открыта ли вторая позиция {is_position_second}')
            # await out_blue(f'Текущее время: {await TimeNow()}, цена монеты один {coint[0].iloc[-1]}, цена монеты два {coint[1].iloc[-1]}, пара: {df1.columns[0]}-{df2.columns[0]}, стратегия: strategy_three, конечная сумма: {round(final_sum, 4)}, прибыль: {round(final_sum, 4) - final_sum_begin}, в процентах {round((final_sum - final_sum_begin) / final_sum_begin * 100, 4)}, открытали первая позиция {is_position_first}, открытали вторая позиция {is_position_second}')
            # await out_blue(f'Пара: {df1.columns[0]}-{df2.columns[0]}, стратегия: strategy_three, конечная сумма: {round(final_sum, 4)}, прибыль: {round(final_sum, 4) - final_sum_begin}, в процентах {round((final_sum - final_sum_begin) / final_sum_begin * 100, 4)}')
            trade_data.to_csv(f'Live_cointegraation_trading.csv', index=False)
            trade_deal.to_csv(f'Live_cointegraation_trading_deals.csv', index=False)
            new_bd.to_csv(f'New_bd.csv', index=False)
            await asyncio.sleep(0.3)

        except Exception as err:
            print(traceback.format_exc())
            df_error.loc[-1] = [datetime.now().strftime('%Y-%m-%d %H:%M:%S'), traceback.format_exc()]
            df_error.index = df_error.index + 1
            df_error = df_error.sort_index()
            df_error.to_csv(r'df_error.csv', index=False)



async def main():
    # print(1)
    trade_data = pd.DataFrame([], columns=['Время', 'Тип акций', 'Тип стратегии', 'Монета_1', 'Монета_2', 'Т-статистика',
                                           'Начальная сумма', 'Конечная сумма', 'Заработок в процентах'])
    trade_deal = pd.DataFrame([], columns=['Время', 'Тип акций', 'Тип стратегии', 'Монета','Тип сделки', 'По какой цене'])
    df_error = pd.DataFrame([], columns=['Время ошибки', 'Тип и место ошибки'])
    new_bd = pd.DataFrame([], columns=['Время', 'Тип акций', 'Тип стратегии', 'Монета', 'Обновили бд', 'BD'])

    start_crypto = '2019-08-30'
    start_stock = '2013-08-30'
    end = datetime.now().strftime("%Y-%m-%d")
    # print(end)
    interval = '1d'
    interval_moex = 24
    async with asyncio.TaskGroup() as tg:
        tg.create_task(strategy_one(trade_data, trade_deal, df_error, new_bd, 'crypto', start_crypto, end, interval, 'QNT-USD', 'PHB-USD'))
        # tg.create_task(strategy_two(trade_data, trade_deal,  df_error,  new_bd,  'crypto', start_crypto, end, interval, 'ARB-USD', 'ICX-USD'))
        tg.create_task(strategy_two(trade_data, trade_deal, df_error, new_bd, 'crypto', start_crypto, end, interval, 'ARB-USD','BNB-USD'))
        # tg.create_task(strategy_three(trade_data, trade_deal, df_error, new_bd, 'crypto', start_crypto, end, interval, 'OMG-USD', 'HOT-USD'))
        tg.create_task(strategy_three(trade_data, trade_deal, df_error,  new_bd,  'crypto', start_crypto, end, interval, 'ARB-USD', 'BAT-USD'))

        tg.create_task(strategy_one(trade_data, trade_deal,  df_error,  new_bd,  'moex', start_stock, end, interval_moex, 'IGST', 'VRSB'))
        # tg.create_task(strategy_two(trade_data, trade_deal, df_error, new_bd,    'moex', start_stock, end, interval_moex, 'VSYD', 'KRKOP'))
        tg.create_task(strategy_two(trade_data, trade_deal,  df_error,  new_bd,  'moex', start_stock, end, interval_moex, 'ABRD', 'VJGZ'))
        # tg.create_task(strategy_three(trade_data, trade_deal, df_error, new_bd,    'moex', start_stock, end, interval_moex, 'VGSBP', 'VRSBP'))
        tg.create_task(strategy_three(trade_data, trade_deal,  df_error,  new_bd,  'moex', start_stock, end, interval_moex, 'VRSB', 'BLNG'))

        tg.create_task(strategy_one(trade_data, trade_deal,   df_error,  new_bd,  'usa', start_stock, end, interval, 'XLY', 'MVIS'))
        tg.create_task(strategy_two(trade_data, trade_deal,   df_error,  new_bd,  'usa', start_stock, end, interval, 'UDOW', 'VTNR'))
        tg.create_task(strategy_two(trade_data, trade_deal, df_error, new_bd, 'usa', start_stock, end, interval, 'TQQQ', 'MVIS'))
        tg.create_task(strategy_two(trade_data, trade_deal, df_error, new_bd, 'usa', start_stock, end, interval, 'SLV', 'MVIS'))
        # tg.create_task(strategy_three(trade_data, trade_deal, df_error, new_bd,  'usa', start_stock, end, interval, 'SOXL', 'MVIS'))
        tg.create_task(strategy_three(trade_data, trade_deal, df_error,  new_bd,  'usa', start_stock, end, interval, 'TECL', 'LOW'))

if __name__ == "__main__":
    df_error = pd.DataFrame([], columns=['Время ошибки', 'Тип и место ошибки'])
    try:
    # asyncio.run(main())
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
        loop.close()
    # loop = asyncio.get_event_loop()  # создаем цикл
    # task = loop.create_task(main())
    # loop.run_until_complete(task)  # ждем окончания выполнения цикла
    except Exception as err:
        print(traceback.format_exc())
        df_error.loc[-1] = [datetime.now().strftime('%Y-%m-%d %H:%M:%S'), traceback.format_exc()]
        df_error.index = df_error.index + 1
        df_error = df_error.sort_index()
        df_error.to_csv(r'df_error.csv',index=False)
