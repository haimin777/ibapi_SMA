from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.contract import Contract as IBcontract
from threading import Thread
import queue
import datetime
import time
import numpy as np
import pandas as pd
import talib

DEFAULT_HISTORIC_DATA_ID=50
DEFAULT_GET_CONTRACT_ID=43

ACCOUNT_UPDATE_FLAG = "update"
ACCOUNT_VALUE_FLAG = "value"
ACCOUNT_TIME_FLAG = "time"

## marker for when queue is finished
FINISHED = object()
STARTED = object()
TIME_OUT = object()


class identifed_as(object):
    # сортировка ответов от api
    def __init__(self, label, data):
        self.label = label
        self.data = data

    def __repr__(self):
        return "Identified as %s" % self.label

        # общий список распознаных ответов от api


class list_of_identified_items(list):
    """
    A list of elements, each of class identified_as (or duck equivalent)

    Used to seperate out accounting data
    """

    def seperate_into_dict(self):
        """

        :return: dict, keys are labels, each element is a list of items matching label
        """

        all_labels = [element.label for element in self]
        dict_data = dict([
            (label,
             [element.data for element in self if element.label == label])
            for label in all_labels])

        return dict_data

class tradeLogic(object):

    def __init__(self):

        pass

    def cross_signal(self, historic_data):
        df = pd.DataFrame(historic_data, columns=('time',
                                                  'open', 'hight',
                                                  'low', 'close',
                                                  'vol'))

        data = np.array(df.close)


        ma_short = talib.SMA(data, timeperiod=20)[-1]
        ma_long = talib.SMA(data, timeperiod=50)[-1]

        allow = ma_short > ma_long
        return allow

    def trade_logic(self, position, signal, pos_vol):

        print("allow: ", signal, '\n', 'position: ', position)
        # Обновляем дынные по позициям

        if signal:  # проверка пересечения
            print("\n", "signal to open long position", "\n")
            if position == 0:
                print('open long')
                #self.ib_order = OrderIB.create_order('MKT', pos_volume, 'BUY')

            elif position < 0:  # выставляем ордер с учетом перекрытия текущей позиции
                print('reverse short')
                #self.ib_order = OrderIB.create_order("MKT", abs(self.position) + pos_volume, "BUY")
            #return self.ib_order
        elif not signal:
            print("\n", "signal to open short position", "\n")
            if position == 0:
                #self.ib_order = OrderIB.create_order('MKT', pos_volume, 'SELL')
                print('open short')
            elif position > 0:
                # перворачиваем текущую длинную позицию
                #self.ib_order = OrderIB.create_order("MKT", abs(self.position) + pos_volume, "SELL")
                print('reverse long')

            #return self.ib_order

class finishableQueue(object):

    def __init__(self, queue_to_finish):

        self._queue = queue_to_finish
        self.status = STARTED

    def get(self, timeout):
        """
        Returns a list of queue elements once timeout is finished, or a FINISHED flag is received in the queue
        :param timeout: how long to wait before giving up
        :return: list of queue elements
        """
        contents_of_queue=[]
        finished=False

        while not finished:
            try:
                current_element = self._queue.get(timeout=timeout)
                if current_element is FINISHED:
                    finished = True
                    self.status = FINISHED
                else:
                    contents_of_queue.append(current_element)
                    ## keep going and try and get more data

            except queue.Empty:
                ## If we hit a time out it's most probable we're not getting a finished element any time soon
                ## give up and return what we have
                finished = True
                self.status = TIME_OUT


        return contents_of_queue

    def timed_out(self):
        return self.status is TIME_OUT


class simpleCache(object):
    """
    Cache is stored in _cache in nested dict, outer key is accountName, inner key is cache label
    """
    def __init__(self, max_staleness_seconds):
        self._cache = dict()
        self._cache_updated_local_time = dict()

        self._max_staleness_seconds = max_staleness_seconds

    def __repr__(self):
        return "Cache with labels"+",".join(self._cache.keys())

    def update_data(self, accountName):
        raise Exception("You need to set this method in an inherited class")

    def _get_last_updated_time(self, accountName, cache_label):
        if accountName not in self._cache_updated_local_time.keys():
            return None

        if cache_label not in self._cache_updated_local_time[accountName]:
            return None

        return self._cache_updated_local_time[accountName][cache_label]


    def _set_time_of_updated_cache(self, accountName, cache_label):
        # make sure we know when the cache was updated
        if accountName not in self._cache_updated_local_time.keys():
            self._cache_updated_local_time[accountName]={}

        self._cache_updated_local_time[accountName][cache_label] = time.time()


    def _is_data_stale(self, accountName, cache_label, ):
        """
        Check to see if the cached data has been updated recently for a given account and label, or if it's stale

        :return: bool
        """
        STALE = True
        NOT_STALE = False

        last_update = self._get_last_updated_time(accountName, cache_label)

        if last_update is None:
            ## we haven't got any data, so by construction our data is stale
            return STALE

        time_now = time.time()
        time_since_updated = time_now - last_update

        if time_since_updated > self._max_staleness_seconds:
            return STALE
        else:
            ## recently updated
            return NOT_STALE

    def _check_cache_empty(self, accountName, cache_label):
        """

        :param accountName: str
        :param cache_label: str
        :return: bool
        """
        CACHE_EMPTY = True
        CACHE_PRESENT = False

        cache = self._cache
        if accountName not in cache.keys():
            return CACHE_EMPTY

        cache_this_account = cache[accountName]
        if cache_label not in cache_this_account.keys():
            return CACHE_EMPTY

        return CACHE_PRESENT

    def _return_cache_values(self, accountName, cache_label):
        """

        :param accountName: str
        :param cache_label: str
        :return: None or cache contents
        """

        if self._check_cache_empty(accountName, cache_label):
            return None

        return self._cache[accountName][cache_label]


    def _create_cache_element(self, accountName, cache_label):

        cache = self._cache
        if accountName not in cache.keys():
            cache[accountName] = {}

        cache_this_account = cache[accountName]
        if cache_label not in cache_this_account.keys():
            cache[accountName][cache_label] = None


    def get_updated_cache(self, accountName, cache_label):
        """
        Checks for stale cache, updates if needed, returns up to date value

        :param accountName: str
        :param cache_label:  str
        :return: updated part of cache
        """

        if self._is_data_stale(accountName, cache_label) or self._check_cache_empty(accountName, cache_label):
            self.update_data(accountName)

        return self._return_cache_values(accountName, cache_label)


    def update_cache(self, accountName, dict_with_data):
        """

        :param accountName: str
        :param dict_with_data: dict, which has keynames with cache labels
        :return: nothing
        """

        all_labels = dict_with_data.keys()
        for cache_label in all_labels:
            self._create_cache_element(accountName, cache_label)
            self._cache[accountName][cache_label] = dict_with_data[cache_label]
            self._set_time_of_updated_cache(accountName, cache_label)


class TradeWrapper(EWrapper):

    def __init__(self):
        self._my_contract_details = {}
        self._my_historic_data_dict = {}
        #на случай нескольких аккаунтов используем словарь
        self._my_accounts = {}

        ## We set these up as we could get things coming along before we run an init
        self._my_positions = queue.Queue()
        self._my_errors = queue.Queue()

    def init_error(self):
        error_queue=queue.Queue()
        self._my_errors = error_queue

    def get_error(self, timeout=5):
        if self.is_error():
            try:
                return self._my_errors.get(timeout=timeout)
            except queue.Empty:
                return None

        return None

    def is_error(self):
        an_error_if=not self._my_errors.empty()
        return an_error_if

    def error(self, id, errorCode, errorString):
        ## Overriden method
        errormsg = "IB error id %d errorcode %d string %s" % (id, errorCode, errorString)
        self._my_errors.put(errormsg)

    ## get positions code
    def init_positions(self):
        positions_queue = self._my_positions = queue.Queue()

        return positions_queue

    def position(self, account, contract, position,
                 avgCost):

        ## uses a simple tuple, but you could do other, fancier, things here
        position_object = (account, contract, position,
                 avgCost)

        self._my_positions.put(position_object)

    def positionEnd(self):
        ## overriden method

        self._my_positions.put(FINISHED)


    ## get accounting data
    def init_accounts(self, accountName):
        accounting_queue = self._my_accounts[accountName] = queue.Queue()

        return accounting_queue


    def updateAccountValue(self, key:str, val:str, currency:str,
                            accountName:str):

        ## use this to seperate out different account data
        data = identifed_as(ACCOUNT_VALUE_FLAG, (key,val, currency))
        self._my_accounts[accountName].put(data)


    def updatePortfolio(self, contract, position:float,
                        marketPrice:float, marketValue:float,
                        averageCost:float, unrealizedPNL:float,
                        realizedPNL:float, accountName:str):

        ## use this to seperate out different account data
        data = identifed_as(ACCOUNT_UPDATE_FLAG, (contract, position, marketPrice, marketValue, averageCost,
                                          unrealizedPNL, realizedPNL))
        self._my_accounts[accountName].put(data)

    def updateAccountTime(self, timeStamp:str):

        ## use this to seperate out different account data
        data = identifed_as(ACCOUNT_TIME_FLAG, timeStamp)
        self._my_accounts[accountName].put(data)


    def accountDownloadEnd(self, accountName:str):

        self._my_accounts[accountName].put(FINISHED)

    #Исторические данные

    ## get contract details code
    def init_contractdetails(self, reqId):
        contract_details_queue = self._my_contract_details[reqId] = queue.Queue()

        return contract_details_queue

    def contractDetails(self, reqId, contractDetails):
        ## overridden method

        if reqId not in self._my_contract_details.keys():
            self.init_contractdetails(reqId)

        self._my_contract_details[reqId].put(contractDetails)

    def contractDetailsEnd(self, reqId):
        ## overriden method
        if reqId not in self._my_contract_details.keys():
            self.init_contractdetails(reqId)

        self._my_contract_details[reqId].put(FINISHED)

    ## Historic data code
    def init_historicprices(self, tickerid):
        historic_data_queue = self._my_historic_data_dict[tickerid] = queue.Queue()

        return historic_data_queue


    def historicalData(self, tickerid , bar):

        ## Overriden method
        ## Note I'm choosing to ignore barCount, WAP and hasGaps but you could use them if you like
        bardata=(bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume)

        historic_data_dict=self._my_historic_data_dict

        ## Add on to the current data
        if tickerid not in historic_data_dict.keys():
            self.init_historicprices(tickerid)

        historic_data_dict[tickerid].put(bardata)

    def historicalDataEnd(self, tickerid, start:str, end:str):
        ## overriden method

        if tickerid not in self._my_historic_data_dict.keys():
            self.init_historicprices(tickerid)

        self._my_historic_data_dict[tickerid].put(FINISHED)


class TradeClient(EClient):
    """
    The client method
    We don't override native methods, but instead call them from our own wrappers
    """

    def __init__(self, wrapper):
        ## Set up with a wrapper inside
        EClient.__init__(self, wrapper)

        ## We use these to store accounting data
        self._account_cache = simpleCache(max_staleness_seconds=5 * 60)
        ## override function
        self._account_cache.update_data = self._update_accounting_data

    def get_current_positions(self):
        """
        Current positions held

        :return:
        """

        ## Make a place to store the data we're going to return
        positions_queue = finishableQueue(self.init_positions())

        ## ask for the data
        self.reqPositions()

        ## poll until we get a termination or die of boredom
        MAX_WAIT_SECONDS = 10
        positions_list = positions_queue.get(timeout=MAX_WAIT_SECONDS)

        while self.wrapper.is_error():
            print(self.get_error())

        if positions_queue.timed_out():
            print("Exceeded maximum wait for wrapper to confirm finished whilst getting positions")

        return positions_list

    def _update_accounting_data(self, accountName):
        """
        Update the accounting data in the cache

        :param accountName: account we want to get data for
        :return: nothing
        """

        ## Make a place to store the data we're going to return
        accounting_queue = finishableQueue(self.init_accounts(accountName))

        ## ask for the data
        self.reqAccountUpdates(True, accountName)

        ## poll until we get a termination or die of boredom
        MAX_WAIT_SECONDS = 10
        accounting_list = accounting_queue.get(timeout=MAX_WAIT_SECONDS)

        while self.wrapper.is_error():
            print(self.get_error())

        if accounting_queue.timed_out():
            print("Exceeded maximum wait for wrapper to confirm finished whilst getting accounting data")

        # seperate things out, because this is one big queue of data with different things in it
        accounting_list = list_of_identified_items(accounting_list)
        seperated_accounting_data = accounting_list.seperate_into_dict()

        ## update the cache with different elements
        self._account_cache.update_cache(accountName, seperated_accounting_data)

        ## return nothing, information is accessed via get_... methods

    def get_accounting_time_from_server(self, accountName):
        """
        Get the accounting time from IB server

        :return: accounting time as served up by IB
        """

        # All these functions follow the same pattern: check if stale or missing, if not return cache, else update values

        return self._account_cache.get_updated_cache(accountName, ACCOUNT_TIME_FLAG)

    def get_accounting_values(self, accountName):
        """
        Get the accounting values from IB server

        :return: accounting values as served up by IB
        """

        # All these functions follow the same pattern: check if stale, if not return cache, else update values

        return self._account_cache.get_updated_cache(accountName, ACCOUNT_VALUE_FLAG)

    def get_accounting_updates(self, accountName):
        """
        Get the accounting updates from IB server

        :return: accounting updates as served up by IB
        """

        # All these functions follow the same pattern: check if stale, if not return cache, else update values

        return self._account_cache.get_updated_cache(accountName, ACCOUNT_UPDATE_FLAG)

    def resolve_ib_contract(self, ibcontract, reqId=DEFAULT_GET_CONTRACT_ID):

        """
        From a partially formed contract, returns a fully fledged version
        :returns fully resolved IB contract
        """

        ## Make a place to store the data we're going to return
        # cjздаем декоратор для приема данных
        contract_details_queue = finishableQueue(self.init_contractdetails(reqId))

        print("Getting full contract details from the server... ")
        # запрашиваем данные контракта у api

        self.reqContractDetails(reqId, ibcontract)

        ## Run until we get a valid contract(s) or get bored waiting
        MAX_WAIT_SECONDS = 10
        # Получаем значение из созданной очереди по оканчании иди таймауту
        new_contract_details = contract_details_queue.get(timeout=MAX_WAIT_SECONDS)
        # если есть ошибки то возвращаем их

        while self.wrapper.is_error():
            print(self.get_error())

        if contract_details_queue.timed_out():
            print("Exceeded maximum wait for wrapper to confirm finished - seems to be normal behaviour")

        if len(new_contract_details) == 0:
            print("Failed to get additional contract details: returning unresolved contract")
            return ibcontract

        if len(new_contract_details) > 1:
            print("got multiple contracts using first one")

        new_contract_details = new_contract_details[0]

        resolved_ibcontract = new_contract_details.summary
        print(resolved_ibcontract)

        return resolved_ibcontract

    def get_IB_historical_data(self, ibcontract, durationStr="1 D", barSizeSetting="5 mins",
                               tickerid=DEFAULT_HISTORIC_DATA_ID):

        """
        Returns historical prices for a contract, up to today
        ibcontract is a Contract
        :returns list of prices in 4 tuples: Open high low close volume
        """

        ## Make a place to store the data we're going to return
        historic_data_queue = finishableQueue(self.init_historicprices(tickerid))

        # Request some historical data. Native method in EClient
        self.reqHistoricalData(
            tickerid,  # tickerId,
            ibcontract,  # contract,
            datetime.datetime.today().strftime("%Y%m%d %H:%M:%S %Z"),  # endDateTime,
            durationStr,  # durationStr,
            barSizeSetting,  # barSizeSetting,
            "MIDPOINT",  # whatToShow,
            1,  # useRTH,
            1,  # formatDate
            False,  # KeepUpToDate <<==== added for api 9.73.2
            []  ## chartoptions not used
        )

        ## Wait until we get a completed data, an error, or get bored waiting
        MAX_WAIT_SECONDS = 20
        print("Getting historical data from the server... could take %d seconds to complete " % MAX_WAIT_SECONDS)

        historic_data = historic_data_queue.get(timeout=MAX_WAIT_SECONDS)

        while self.wrapper.is_error():
            print(self.get_error())

        if historic_data_queue.timed_out():
            print("Exceeded maximum wait for wrapper to confirm finished - seems to be normal behaviour")

        self.cancelHistoricalData(tickerid)

        return historic_data


class TradeApp(TradeWrapper, TradeClient):
    def __init__(self, ipaddress, portid, clientid):
        TradeWrapper.__init__(self)
        TradeClient.__init__(self, wrapper=self)

        self.connect(ipaddress, portid, clientid)

        thread = Thread(target=self.run)
        thread.start()

        setattr(self, "_thread", thread)
if __name__ == '__main__':

    app = TradeApp("127.0.0.1", 7497, 0)
    tr = tradeLogic()
    ibcontract = IBcontract()
    ibcontract.symbol = "EUR"
    ibcontract.secType = "CASH"
    ibcontract.currency = "GBP"
    ibcontract.exchange = "IDEALPRO"
    ## lets get positions
    positions_list = app.get_current_positions()
    print(positions_list)
    print('pos 1', positions_list[0][2])

    ## get the account name from the position
    ## normally you would know your account name
    accountName = positions_list[0][0]
    ## and accounting information
    accounting_values = app.get_accounting_values(accountName)
    print(accounting_values)
    ## these values are cached
    ## if we ask again in more than 5 minutes it will update everything
    accounting_updates = app.get_accounting_updates(accountName)
    print(accounting_updates)

    try:
        while True:
            resolved_ibcontract = app.resolve_ib_contract(ibcontract)
            historic_data = app.get_IB_historical_data(resolved_ibcontract)

            signal = tr.cross_signal(historic_data)

            tr.trade_logic(positions_list[0][2], signal, 20000)
            time.sleep(30)

    finally:
        app.disconnect()






