from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.order import Order
from ibapi.contract import Contract as IBcontract
from helpers import identifed_as, list_of_identified_items, SimpleCache
from trade_logic import TradeLogic
from threading import Thread
import queue
import datetime
import time

DEFAULT_HISTORIC_DATA_ID = 50
DEFAULT_GET_CONTRACT_ID = 43

ACCOUNT_UPDATE_FLAG = "update"
ACCOUNT_VALUE_FLAG = "value"
ACCOUNT_TIME_FLAG = "time"

## marker for when queue is finished
FINISHED = object()
STARTED = object()
TIME_OUT = object()


class FinishableQueue(object):
    def __init__(self, queue_to_finish):

        self._queue = queue_to_finish
        self.status = STARTED

    def get(self, timeout):
        """
        Returns a list of queue elements once timeout is finished, or a FINISHED flag is received in the queue
        :param timeout: how long to wait before giving up
        :return: list of queue elements
        """
        contents_of_queue = []
        finished = False

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


class TradeWrapper(EWrapper):
    def __init__(self):
        self._my_contract_details = {}
        self._my_historic_data_dict = {}
        # на случай нескольких аккаунтов используем словарь
        self._my_accounts = {}

        ## We set these up as we could get things coming along before we run an init
        self._my_positions = queue.Queue()
        self._my_errors = queue.Queue()

    def init_error(self):
        error_queue = queue.Queue()
        self._my_errors = error_queue

    def get_error(self, timeout=5):
        if self.is_error():
            try:
                return self._my_errors.get(timeout=timeout)
            except queue.Empty:
                return None

        return None

    def is_error(self):
        an_error_if = not self._my_errors.empty()
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
        position_object = (account, contract.localSymbol, position,
                           avgCost)

        self._my_positions.put(position_object)

    def positionEnd(self):
        ## overriden method

        self._my_positions.put(FINISHED)

    ## get accounting data
    def init_accounts(self, accountName):
        self._my_accounts[accountName] = queue.Queue()
        accounting_queue = self._my_accounts[accountName]

        return accounting_queue

    def updateAccountValue(self, key: str, val: str, currency: str,
                           accountName: str):

        ## use this to seperate out different account data
        data = identifed_as(ACCOUNT_VALUE_FLAG, (key, val, currency))
        self._my_accounts[accountName].put(data)

    def updatePortfolio(self, contract, position: float,
                        marketPrice: float, marketValue: float,
                        averageCost: float, unrealizedPNL: float,
                        realizedPNL: float, accountName: str):

        ## use this to seperate out different account data
        data = identifed_as(ACCOUNT_UPDATE_FLAG, (contract, position, marketPrice, marketValue, averageCost,
                                                  unrealizedPNL, realizedPNL))
        self._my_accounts[accountName].put(data)

    def updateAccountTime(self, timeStamp: str):

        ## use this to seperate out different account data
        data = identifed_as(ACCOUNT_TIME_FLAG, timeStamp)
        self._my_accounts[accountName].put(data)

    def accountDownloadEnd(self, accountName: str):

        self._my_accounts[accountName].put(FINISHED)

    # Исторические данные

    ## get contract details code
    def init_contractdetails(self, reqId):
        self._my_contract_details[reqId] = queue.Queue()
        contract_details_queue = self._my_contract_details[reqId]

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

    def historicalData(self, tickerid, bar):

        ## Overriden method
        ## Note I'm choosing to ignore barCount, WAP and hasGaps but you could use them if you like
        bardata = (bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume)

        historic_data_dict = self._my_historic_data_dict

        ## Add on to the current data
        if tickerid not in historic_data_dict.keys():
            self.init_historicprices(tickerid)

        historic_data_dict[tickerid].put(bardata)

    def historicalDataEnd(self, tickerid, start: str, end: str):
        ## overriden method

        if tickerid not in self._my_historic_data_dict.keys():
            self.init_historicprices(tickerid)

        self._my_historic_data_dict[tickerid].put(FINISHED)

        # order id receiving

    def init_nextvalidid(self):

        orderid_queue = self._my_orderid_data = queue.Queue()

        return orderid_queue

    def nextValidId(self, orderId):

        if getattr(self, '_my_orderid_data', None) is None:
            self.init_nextvalidid()

        self._my_orderid_data.put(orderId)


class TradeClient(EClient):
    """
    The client method
    We don't override native methods, but instead call them from our own wrappers
    """

    def __init__(self, wrapper):
        ## Set up with a wrapper inside
        EClient.__init__(self, wrapper)

        ## We use these to store accounting data
        self._account_cache = SimpleCache(max_staleness_seconds=5 * 60)
        ## override function
        self._account_cache.update_data = self._update_accounting_data

    def get_current_positions(self):
        """
        Current positions held
        :return:
        """

        ## Make a place to store the data we're going to return
        positions_queue = FinishableQueue(self.init_positions())

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
        accounting_queue = FinishableQueue(self.init_accounts(accountName))

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
        contract_details_queue = FinishableQueue(self.init_contractdetails(reqId))

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
        # print(resolved_ibcontract)

        return resolved_ibcontract

    def get_next_brokerorderid(self):
        """
        Get next broker order id
        :return: broker order id, int; or TIME_OUT if unavailable
        """

        ## Make a place to store the data we're going to return
        orderid_q = self.init_nextvalidid()

        self.reqIds(-1)  # -1 is irrelevant apparently (see IB API docs)

        ## Run until we get a valid contract(s) or get bored waiting
        MAX_WAIT_SECONDS = 10
        try:
            brokerorderid = orderid_q.get(timeout=MAX_WAIT_SECONDS)
        except queue.Empty:
            print("Wrapper timeout waiting for broker orderid")
            brokerorderid = TIME_OUT

        while self.wrapper.is_error():
            print(self.get_error(timeout=MAX_WAIT_SECONDS))

            return brokerorderid

    def get_IB_historical_data(self, ibcontract, durationStr="1 D", barSizeSetting="5 mins",
                               tickerid=DEFAULT_HISTORIC_DATA_ID):

        """
        Returns historical prices for a contract, up to today
        ibcontract is a Contract
        :returns list of prices in 4 tuples: Open high low close volume
        """

        ## Make a place to store the data we're going to return
        historic_data_queue = FinishableQueue(self.init_historicprices(tickerid))

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

    def place_new_IB_order(self, ibcontract, order, orderid=None):

        ## We can eithier supply our own ID or ask IB to give us the next valid one

        if orderid is None:
            print("Getting orderid from IB")
            orderid = self.get_next_brokerorderid()

            if orderid is TIME_OUT:
                raise Exception("I couldn't get an orderid from IB, and you didn't provide an orderid")

        print("Using order id:", orderid)

        # Place the order

        self.placeOrder(orderid, ibcontract, order)

        return orderid

    def get_positions_dict(self, positions_list):

        positions_dict = {positions_list[i][1]: positions_list[i][2] for i in range(0,
                                                                                    len(positions_list))}

        return positions_dict


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
    tr = TradeLogic()

    ibcontract = tr.create_contract('EUR', 'GBP')

    positions_list = app.get_current_positions()
    print("positions list: ", "\n", positions_list,
          '\n', len(positions_list))

    accountName = positions_list[0][0]
    accounting_values = app.get_accounting_values(accountName)
    accounting_updates = app.get_accounting_updates(accountName)
    print("acc val:", accounting_updates)

    try:
        while True:
            positions_list = app.get_current_positions()
            pos_dict = app.get_positions_dict(positions_list)
            resolved_ibcontract = app.resolve_ib_contract(ibcontract)
            historic_data = app.get_IB_historical_data(resolved_ibcontract)
            signal = tr.cross_signal(historic_data)
            pos = pos_dict.get(str(resolved_ibcontract.localSymbol), 0)

            tr.trade_logic(pos, signal)

            try:

                orderid1 = app.place_new_IB_order(ibcontract, tr.ib_order, orderid=None)
                print("Placed market order, orderid is %d" % orderid1)
                tr.ib_order = None
            except AttributeError:
                print("already placed")

            finally:
                time.sleep(30)

    finally:
        app.disconnect()
