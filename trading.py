import pyupbit
import schedule
import time
import datetime
import logging
import logging.handlers
import requests
import pandas as pd
import json

INTERVAL = 1                                        # 매수 시도 interval (1초 기본)
DEBUG = False                                       # True: 매매 API 호출 안됨, False: 실제로 매매 API 호출
COIN_NUMS = 1                                       # 분산 투자 코인 개수 (자산/COIN_NUMS를 각 코인에 투자)
LARRY_K = 0.5
TRAILLING_STOP_GAP = 0.05                           # 최고점 대비 15% 하락시 매도
RESET_TIME = 20

# logger instance 생성
logger = logging.getLogger(__name__)

# formatter 생성
formatter = logging.Formatter('[%(asctime)s][%(levelname)s|%(filename)s:%(lineno)s] >> %(message)s')

# handler 생성 (stream, file)
streamHandler = logging.StreamHandler()
timedfilehandler = logging.handlers.TimedRotatingFileHandler(filename='logfile', when='midnight', interval=1, encoding='utf-8')

# logger instance에 fomatter 설정
streamHandler.setFormatter(formatter)
timedfilehandler.setFormatter(formatter)
timedfilehandler.suffix = "%Y%m%d"

# logger instance에 handler 설정
logger.addHandler(streamHandler)
logger.addHandler(timedfilehandler)

# logger instnace로 log 찍기
logger.setLevel(level=logging.DEBUG)


# Load account
with open("upbit.txt") as f:
    lines = f.readlines()
    key = lines[0].strip()
    secret = lines[1].strip()
    upbit = pyupbit.Upbit(key, secret)

def make_sell_times(now):
    '''
    금일 09:01:00 시각과 09:01:10초를 만드는 함수
    :param now: DateTime
    :return:
    '''
    today = now
    sell_time = datetime.datetime(year=today.year,
            month=today.month,
            day=today.day,
            hour=9,
            minute=1,
            second=0)
    sell_time_after_10secs = sell_time + datetime.timedelta(seconds=20)
    return sell_time, sell_time_after_10secs


def make_setup_times(now):
    '''
    익일 09:01:00 시각과 09:01:10초를 만드는 함수
    :param now:
    :return:
    '''
    tomorrow = now + datetime.timedelta(1)
    midnight = datetime.datetime(year=tomorrow.year,
            month=tomorrow.month,
            day=tomorrow.day,
            hour=9,
            minute=1,
            second=0)
    midnight_after_10secs = midnight + datetime.timedelta(seconds=20)
    return midnight, midnight_after_10secs

def make_volume_times(now):
    today = now
    sell_time = datetime.datetime(year=today.year,
            month=today.month,
            day=today.day,
            hour=13,
            minute=0,
            second=0)
    return sell_time

def make_portfolio_today_times(now):
    '''
    금일 09:30:00 시각과 09:30:10초를 만드는 함수
    :param now: DateTime
    :return:
    '''
    today = now
    sell_time1 = datetime.datetime(year=today.year,
            month=today.month,
            day=today.day,
            hour=9,
            minute=30,
            second=0)
    sell_time2 = sell_time1 + datetime.timedelta(seconds=10)

    sell_time3 = datetime.datetime(year=today.year,
            month=today.month,
            day=today.day,
            hour=13,
            minute=30,
            second=0)
    sell_time4 = sell_time3 + datetime.timedelta(seconds=10)
    return sell_time1, sell_time2, sell_time3, sell_time4

def get_cur_prices(tickers):
    '''
    모든 가상화폐에 대한 현재가 조회
    :return: 현재가, {'KRW-BTC': 7200000, 'KRW-XRP': 500, ...}
    '''
    try:
        return pyupbit.get_current_price(tickers)
    except:
        return None

def inquiry_high_prices(tickers):
    try:
        high_prices = {}
        for ticker in tickers:
            df = pyupbit.get_ohlcv(ticker, interval="day", count=10)
            today = df.iloc[-1]
            today_high = today['high']
            high_prices[ticker] = today_high

        return high_prices
    except:
        return  {ticker:0 for ticker in tickers}

def cal_target(ticker):
    '''
    각 코인에 대한 목표가 저장
    :param ticker: 티커, 'BTC'
    :return: 목표가
    '''
    try:
        df = pyupbit.get_ohlcv(ticker, interval="day", count=10)
        yesterday = df.iloc[-2]
        today_open = yesterday['close']
        yesterday_high = yesterday['high']
        yesterday_low = yesterday['low']
        target = today_open + (yesterday_high - yesterday_low) * LARRY_K
        return today_open, target
    except Exception as e:
        logger.error('cal_target Exception occur')
        logger.error(e)
        return None, None


def set_targets(tickers):
    '''
    티커 코인들에 대한 목표가 계산
    :param tickers: 코인에 대한 티커 리스트
    :return:
    '''
    closes = {}
    targets = {}
    for ticker in tickers:
        closes[ticker], targets[ticker] = cal_target(ticker)
        time.sleep(0.1)
    return closes, targets

def cal_volume(ticker):
    '''
    각 코인에 대한 전일대비 거래량 저장
    :param ticker: 티커, 'BTC'
    :return: 전일대비 거래량
    '''
    try:
        df = pyupbit.get_ohlcv(ticker, interval="day", count=10)
        yesterday = df.iloc[-2]
        today = df.iloc[-1]
        yesterday_volume = yesterday['volume']
        today_volume = today['volume']
        volume_ratio = today_volume / yesterday_volume
        return volume_ratio
    except Exception as e:
        logger.error('cal_volume Exception occur')
        logger.error(e)
        return 0

def set_volumes(tickers):
    '''
    티커 코인들에 대한 전일 대비 거래량 계산
    :param tickers: 코인에 대한 티커 리스트
    :return:
    '''
    volumes = {}
    for ticker in tickers:
        volumes[ticker] = cal_volume(ticker)
        time.sleep(0.1)
    return volumes


def get_portfolio(tickers, prices, targets, blackList):
    '''
    매수 조건 확인 및 매수 시도
    :param tickers: 코인 리스트
    :param prices: 각 코인에 대한 현재가
    :param targets: 각 코인에 대한 목표가
    :return:
    '''
    portfolio = []
    try:
        for ticker in tickers:
            price = prices[ticker]              # 현재가
            target = targets[ticker]            # 목표가

            # 현재가가 목표가 이상이고
            if price >= target and blackList[ticker] is False:
                portfolio.append(ticker)

        return portfolio
    except Exception as e:
        logger.error('get_portfolio Exception occur')
        logger.error(e)
        return None

def buy_volume(volume_list, prices, targets, volume_holdings, budget_per_coin, blackList, high_prices):
    '''
    매수 조건 확인 및 매수 시도
    :param volume_list: 거래량 리스트
    :param volume_holdings: 거래량 기준 보유 여부 
    :param budget_per_coin: 코인별 최대 투자 금액
    :return:
    '''
    try:
        for ticker in volume_list:
            tick = ticker[0]
            high = high_prices[tick]
            target = targets[tick]
            price = prices[tick]

            logger.info('-----buy_volume()-----')
            logger.info('ticker')
            logger.info(ticker)
            logger.info('price')
            logger.info(price)
            logger.info('high')
            logger.info(high)
            logger.info('target*1.02')
            logger.info(target*1.02)


            # 현재 보유하지 않은 상태 
            # 한 번도 매도하지 않은 상태 
            # 한 번도 익절하지 않은 상태
            # 전일 대비 거래량 0% 이상일 경우
            # 현재가가 고가보다 높은 경우
            # 고가가 목표가 대비 2% 이상 오르지 않은 경우
            #if volume_holdings[tick] is False and blackList[tick] is False and ticker[1] > 0 and price >= high and price < target * 1.02: 
            if volume_holdings[tick] is False: 
                logger.info('Ticker')
                logger.info(ticker)
                fee = budget_per_coin * 0.0005

                if DEBUG is False:
                    upbit.buy_market_order(tick, budget_per_coin - fee)
                else:
                    logger.info('BUY VOLUME')
                    print("BUY API CALLED", tick)

                time.sleep(INTERVAL)
    except Exception as e:
        logger.error('buy_volume Exception occur')
        logger.error(e)

# 잔고 조회
def get_balance_unit(tickers):
    balances = upbit.get_balances()
    units = {ticker:0 for ticker in tickers}

    for balance in balances:
        if balance['currency'] == "KRW":
            continue

        ticker = "KRW-" + balance['currency']        # XRP -> KRW-XRP
        unit = float(balance['balance'])
        units[ticker] = unit
    return units


def sell_holdings(tickers, portfolio, prices, targets, blackList):
    '''
    보유하고 있는 모든 코인에 대해 전량 매도
    :param tickers: 업비트에서 지원하는 암호화폐의 티커 목록
    :return:
    '''
    try:
        # 잔고조회
        units = get_balance_unit(tickers)

        for ticker in tickers:
            unit = units.get(ticker, 0)                     # 보유 수량
            price = prices[ticker]
            target = targets[ticker]
            gain = (price-target)/target

            if unit > 0:
                orderbook = pyupbit.get_orderbook(ticker)['orderbook_units'][0]
                buy_price = int(orderbook['bid_price'])                                 # 최우선 매수가
                buy_unit = orderbook['bid_size']                                        # 최우선 매수수량
                min_unit = min(unit, buy_unit)

                # 보유 중인 코인이 포트폴리오에 없으면 매도한다.
                if ticker not in portfolio:
                    if DEBUG is False:
                        upbit.sell_market_order(ticker, unit)
                    else:
                        print("SELL HOLDINGS API CALLED", ticker, buy_price, min_unit)

                # 손실이 -2%를 넘으면 매도한다.
                if gain <= -0.02:
                    if DEBUG is False:
                        upbit.sell_market_order(ticker, unit)
                    else:
                        print("SELL HOLDINGS API CALLED", ticker, buy_price, min_unit)

                #blackList[ticker] = True

    except Exception as e:
        logger.error('sell_holdings Exception occur')
        logger.error(e)



def try_sell(tickers):
    '''
    보유하고 있는 모든 코인에 대해 전량 매도
    :param tickers: 업비트에서 지원하는 암호화폐의 티커 목록
    :return:
    '''
    try:
        # 잔고조회
        units = get_balance_unit(tickers)

        logger.info('----------try_sell(tickers)---------')
        logger.info('try_sell before sell units')
        logger.info(units)

        for ticker in tickers:
            short_ticker = ticker.split('-')[1]
            unit = units.get(ticker, 0)                     # 보유 수량

            logger.info('-----------ticker-----------')
            logger.info(ticker)
            logger.info('-----------try_sell unit---------')
            logger.info(unit)

            if unit > 0:
                orderbook = pyupbit.get_orderbook(ticker)['orderbook_units'][0]
                logger.info('----------orderbook----------')
                logger.info(orderbook)
                buy_price = int(orderbook['bid_price'])                                 # 최우선 매수가
                buy_unit = orderbook['bid_size']                                        # 최우선 매수수량
                min_unit = min(unit, buy_unit)

                if DEBUG is False:
                    ret = upbit.sell_limit_order(ticker, buy_price, min_unit)
                    logger.info('----------sell_limit_order ret-----------')
                    logger.info(ret)
                    time.sleep(INTERVAL)

                    if ret is None:
                        pyupbit.sell_market_order(ticker, unit)
                else:
                    print("SELL API CALLED", ticker, buy_price, min_unit)

        logger.info('try_sell after sell units')
        logger.info(units)

    except Exception as e:
        logger.error('try_sell Exception occur')
        logger.error(e)

def sell(ticker, unit):
    orderbook = pyupbit.get_orderbook(ticker)['orderbook_units'][0]
    buy_price = int(orderbook['bid_price'])                                 # 최우선 매수가
    buy_unit = orderbook['bid_size']                                        # 최우선 매수수량

    if DEBUG is False:
        pyupbit.sell_market_order(tick, unit)
    else:
        print("trailing stop", tick, buy_price, unit)

def try_trailling_stop(volume_list, prices, closes, targets, high_prices, blackList):
    '''
    trailling stop
    :param portfolio: 포트폴리오
    :param prices: 현재가 리스트
    :param closes: 전일 종가
    :param targets: 목표가 리스트
    :param high_prices: 각 코인에 대한 당일 최고가 리스트
    :return:
    '''
    try:
        # 잔고 조회
        units = get_balance_unit(tickers)

        for ticker in volume_list:
            tick = ticker[0]
            price = prices[tick]                          # 현재가
            target = targets[tick]                        # 목표가
            high_price = high_prices[tick]                # 당일 최고가
            close = closes[tick]                          # 전일 종가
            span_a = spans_a[tick]                        # 선행 스팬 1
            span_b = spans_b[tick]                        # 선행 스팬 2
            unit = units.get(tick, 0)                     # 보유 수량

            gain = (price - target) / target                # 이익률
            ascent = (price - close) / close                # 상승률
            gap_from_high = 1 - (price/high_price)          # 고점과 현재가 사이의 갭

            if unit > 0:
                logger.info('TRY_TRAILLING_STOP() Ticker')
                logger.info(tick)
                if ticker[1] < 1 and gain * 100 >= 0.5:
                    logger.info('Condition 1')
                    sell(tick, unit)
                    blackList[tick] = True

                elif ticker[1] >= 1 and ticker[1] < 5 and gain * 100 >= 1 :
                    logger.info('Condition 2')
                    sell(tick, unit)
                    blackList[tick] = True

                elif ticker[1] >= 5 and ticker[1] < 10 and gain * 100 >= 1.5 :
                    logger.info('Condition 2')
                    sell(tick, unit)
                    blackList[tick] = True

                elif ticker[1] >= 10 and gain * 100 >= 10:
                    logger.info('Condition 3')
                    sell(tick, unit)
                    blackList[tick] = True

    except Exception as e:
        logger.error('try trailing stop error')
        logger.error(e)

def set_budget():
    '''
    한 코인에 대해 투자할 투자 금액 계산
    :return: 원화잔고/투자 코인 수
    '''
    try:
        balance = upbit.get_balances()[0]
        krw_balance = 0

        if balance['currency'] == 'KRW':
            krw_balance = float(balance['balance'])
        print("-----set_budget()-----")
        print(krw_balance)

        balances = upbit.get_balances()
        holding_count = len(balances) - 2

        if COIN_NUMS - holding_count > 0:
            return int(krw_balance / (COIN_NUMS - holding_count))
        else:
            return 0
    except Exception as e:
        logger.error('set_budget Exception occur')
        logger.error(e)
        return 0

def set_holdings(tickers):
    '''
    현재 보유 중인 종목
    :return: 보유 종목 리스트
    '''
    try:
        units = get_balance_unit(tickers)                   # 잔고 조회
        holdings = {ticker:False for ticker in tickers}        

        for ticker in tickers:
            unit = units.get(ticker, 0)                     # 보유 수량

            if unit > 0:
                holdings[ticker] = True

        return holdings
    except Exception as e:
        logger.error('set_holdings() Exception error')
        logger.error(e)


def update_high_prices(tickers, high_prices, cur_prices):
    '''
    모든 코인에 대해서 당일 고가를 갱신하여 저장
    :param tickers: 티커 목록 리스트
    :param high_prices: 당일 고가
    :param cur_prices: 현재가
    :return:
    '''
    try:
        for ticker in tickers:
            cur_price = cur_prices[ticker]
            high_price = high_prices[ticker]
            if cur_price > high_price:
                high_prices[ticker] = cur_price
    except:
        pass

def print_status(portfolio, prices, targets, closes):
    '''
    코인별 현재 상태를 출력
    :param tickers: 티커 리스트
    :param prices: 가격 리스트
    :param targets: 목표가 리스트
    :param high_prices: 당일 고가 리스트
    :param kvalues: k값 리스트
    :return:
    '''
    try:
        for ticker in portfolio:
            close = closes[ticker]
            price = prices[ticker]
            target = targets[ticker]
            ascent = (price - close) / close                # 상승률
            gain = (price - target) / target                # 이익률

            logger.info('-------------------------------------------')
            logger.info(ticker)
            logger.info('목표가')
            logger.info(target)
            logger.info('현재가')
            logger.info(price)
            logger.info('상승률')
            logger.info(ascent)
            logger.info('목표가 대비 상승률')
            logger.info(gain)
        logger.info('-------------------------------------------')
    except:
        pass

def reset_orderlist(orderList):
    try:
        logger.info('orderlist reset() RUN')
        logger.info(orderList)
        for order in orderList:
            orderList[order] = False
    except:
        logger.error('orderList reset Exception Occur')
        pass

def get_span(ticker):
    try:
        url = "https://api.upbit.com/v1/candles/days"

        querystring = {"market":ticker,"count":"100"}

        response = requests.request("GET", url, params=querystring)

        data = response.json()

        df = pd.DataFrame(data)

        df=df.iloc[::-1]

        high_prices = df['high_price']
        close_prices = df['trade_price']
        low_prices = df['low_price']
        dates = df.index

        nine_period_high =  df['high_price'].rolling(window=9).max()
        nine_period_low = df['low_price'].rolling(window=9).min()
        df['tenkan_sen'] = (nine_period_high + nine_period_low) /2

        period26_high = high_prices.rolling(window=26).max()
        period26_low = low_prices.rolling(window=26).min()
        df['kijun_sen'] = (period26_high + period26_low) / 2

        df['senkou_span_a'] = ((df['tenkan_sen'] + df['kijun_sen']) / 2).shift(26)

        period52_high = high_prices.rolling(window=52).max()
        period52_low = low_prices.rolling(window=52).min()
        df['senkou_span_b'] = ((period52_high + period52_low) / 2).shift(26)

        df['chikou_span'] = close_prices.shift(-26)

        return df['senkou_span_a'].iloc[-1], df['senkou_span_b'].iloc[-1]
    except Exception as e:
        print('get_span() Exception Occur: ',e)
        return 0, 0

def get_spans(tickers):
    try:
        span_a = {}
        span_b = {}
        time.sleep(1)

        for ticker in tickers:
            span_a[ticker], span_b[ticker] = get_span(ticker)   
            time.sleep(0.1)
    
        return span_a, span_b
    except Exception as e:
        print('get_spans() Exception Occur: ',e)
        return {}, {}


#----------------------------------------------------------------------------------------------------------------------
# 매매 알고리즘 시작
#---------------------------------------------------------------------------------------------------------------------
now = datetime.datetime.now()                                            # 현재 시간 조회
sell_time1, sell_time2 = make_sell_times(now)                            # 초기 매도 시간 설정
setup_time1, setup_time2 = make_setup_times(now)                         # 초기 셋업 시간 설정
portfolio_time1, portfolio_time2, portfolio_time3, portfolio_time4 = make_portfolio_today_times(now)
volume_time = make_volume_times(now)                                     # 오후 거래량 시간 설정

tickers = pyupbit.get_tickers(fiat="KRW")                                # 티커 리스트 얻기
target_tickers = ['KRW-XRP', 'KRW-GMT']

logger.info('-------------------------')
logger.info('sell_time1')
logger.info(sell_time1)
logger.info('sell_time2')
logger.info(sell_time2)
logger.info('setup_time1')
logger.info(setup_time1)
logger.info('setup_time2')
logger.info(setup_time2)
logger.info('volume_time')
logger.info(volume_time)
logger.info('portfolio_time1')
logger.info(portfolio_time1)
logger.info('portfolio_time2')
logger.info(portfolio_time2)
logger.info('portfolio_time3')
logger.info(portfolio_time3)
logger.info('portfolio_time4')
logger.info(portfolio_time4)
logger.info('-------------------------')


closes, targets = set_targets(tickers)                                   # 코인별 목표가 계산

volume_holdings = {ticker:False for ticker in tickers}                   # 전일 대비 거래량 기준 보유 상태 초기화
high_prices = inquiry_high_prices(tickers)                               # 코인별 당일 고가 저장
blackList = {ticker:False for ticker in tickers}                         # 한 번 익절한 코인

spans_a, spans_b = get_spans(tickers)                                    # 일목균형표 선행스팬 1, 2 설정 

volume_list = {}

while True:

    now = datetime.datetime.now()
    schedule.run_pending()                                               # 20분 마다 블랙리스트를 초기화

    # 새로운 거래일에 대한 데이터 셋업 (09:01:00 ~ 09:01:20)
    # 금일, 익일 포함
    if (sell_time1 < now < sell_time2) or (setup_time1 < now < setup_time2):
        logger.info('새로운 거래일 데이터 셋업')
        try_sell(tickers)                                                # 매도 되지 않은 코인에 대해서 한 번 더 매도 시도

        spans_a, spans_b = get_spans(tickers)                            # 일목균형표 선행스팬 1, 2 설정

        setup_time1, setup_time2 = make_setup_times(now)                 # 다음 거래일 셋업 시간 갱신
        volume_time = make_volume_times(now)                             # 오후 거래량 시간 설정
        portfolio_time1, portfolio_time2, portfolio_time3, portfolio_time4 = make_portfolio_today_times(now)

        logger.info('-------------------------')
        logger.info('setup_time1')
        logger.info(setup_time1)
        logger.info('setup_time2')
        logger.info(setup_time2)
        logger.info('volume_time')
        logger.info(volume_time)
        logger.info('portfolio_time1')
        logger.info(portfolio_time1)
        logger.info('portfolio_time2')
        logger.info(portfolio_time2)
        logger.info('portfolio_time3')
        logger.info(portfolio_time3)
        logger.info('portfolio_time4')
        logger.info(portfolio_time4)
        logger.info('-------------------------')

        closes, targets = set_targets(tickers)                           # 목표가 갱신

        logger.info('Targets')
        logger.info(targets)

        volume_holdings = {ticker:False for ticker in tickers}           # 전일 대비 거래량 기준 보유 상태 초기화
        high_prices = {ticker: 0 for ticker in tickers}                  # 코인별 당일 고가 초기화

        blackList = {ticker:False for ticker in tickers}                 # 한 번 익절한 코인

        volume_list = {}                                                 # 전일 대비 거래량 순위

        logger.info('새로운 거래일 데이터 셋업 마무리')
        time.sleep(20)

    prices = get_cur_prices(tickers)                                     # 현재가 계산
    update_high_prices(tickers, high_prices, prices)                     # 고가 갱신

    portfolio = get_portfolio(target_tickers, prices, targets, blackList)       
    volumes = set_volumes(portfolio)                                 # 전일 대비 거래량

    # 전일 대비 거래량 순위
    sorted_volumes = sorted(volumes.items(), key = lambda item: item[1], reverse = True)
    volume_list = sorted_volumes[0:COIN_NUMS]

    logger.info('volume_list')
    logger.info(volume_list)

    volume_portfolio = [x[0] for x in volume_list]

    print_status(volume_portfolio, prices, targets, closes)

    budget_per_coin = set_budget()                                        # 코인별 최대 배팅 금액 계산

    logger.info('blackList')
    for black in blackList:
        if blackList[black]:
            logger.info(black)

    logger.info('budget_per_coin')
    logger.info(budget_per_coin)

    # 매도(손절매)
    # sell_holdings(tickers, volume_portfolio, prices, targets, blackList) 

    # 매수
    volume_holdings = set_holdings(tickers)
    buy_volume(volume_list, prices, targets, volume_holdings, budget_per_coin, blackList, high_prices)

    # 매도(익절)
    '''
    if volume_time < now < setup_time1:
        volume_holdings = set_holdings(tickers)
        try_trailling_stop(volume_list, prices, closes, targets, high_prices, blackList)
    '''

    time.sleep(INTERVAL)
