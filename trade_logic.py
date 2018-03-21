import numpy as np
import pandas as pd
import talib
from ibapi.contract import Contract as IBcontract
from ibapi.order import Order


class TradeLogic(object):
    def __init__(self):
        self.ib_order = None
        self.pos_volume = 5000

    def create_order(self, order_type, quantity, action):
        order = Order()
        order.orderType = order_type
        order.totalQuantity = quantity
        order.action = action
        order.transmit = True

        return order

    def create_contract(self, symbol, currency):

        ibcontract = IBcontract()
        ibcontract.symbol = symbol
        ibcontract.secType = "CASH"
        ibcontract.currency = currency
        ibcontract.exchange = "IDEALPRO"

        return ibcontract

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

    def trade_logic(self, position, signal):

        print("allow: ", signal, '\n', 'position: ', position)
        # Обновляем дынные по позициям
        pos_volume = self.pos_volume
        if signal:  # проверка пересечения
            print("\n", "signal to open long position", "\n")
            if position == 0:
                print('open long')
                self.ib_order = self.create_order('MKT', pos_volume, 'BUY')
                return self.ib_order

            elif position < 0:  # выставляем ордер с учетом перекрытия текущей позиции
                print('reverse short')
                self.ib_order = self.create_order("MKT", abs(position) + pos_volume, "BUY")
                return self.ib_order
        elif not signal:
            print("\n", "signal to open short position", "\n")
            if position == 0:
                self.ib_order = self.create_order('MKT', pos_volume, 'SELL')
                print('open short')
            elif position > 0:
                # перворачиваем текущую длинную позицию
                self.ib_order = self.create_order("MKT", abs(position) + pos_volume, "SELL")
                print('reverse long')

                return self.ib_order
