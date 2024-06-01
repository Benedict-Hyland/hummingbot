import logging
import os
from datetime import datetime
from decimal import Decimal
from typing import Dict

import pandas as pd

from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType
from hummingbot.core.event.event_forwarder import SourceInfoEventForwarder
from hummingbot.core.event.events import OrderBookEvent, OrderBookTradeEvent
from hummingbot.data_feed.candles_feed.candles_factory import CandlesConfig, CandlesFactory
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy.strategy_py_base import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
)


class SimpleOrder(ScriptStrategyBase):
    """
    This example script places an order on a Hummingbot exchange connector. The user can select the
    order type (market or limit), side (buy or sell) and the spread (for limit orders only).
    The bot uses the Rate Oracle to convert the order amount in USD to the base amount for the exchange and trading pair.
    The script uses event handlers to notify the user when the order is created and completed, and then stops the bot.
    """

    # Key Parameters
    exchange = os.getenv("EXCHANGE", "binance_paper_trade")
    trading_pairs = os.getenv("TRADING_PAIRS", 'BTC-FDUSD,ETH-FDUSD')
    depth = int(os.getenv("DEPTH", 50))
    buying_percentage = os.getenv("BUYINGPERCENTAGE", 30)
    spread_buy = os.getenv("SPREAD_BUY", 0.1)
    max_orders = os.getenv("MAX_LIMIT_ORDERS", 4)

    take_profit_percent = Decimal(os.getenv("TP_PERCENT", 0.0751))
    stop_loss_percent = Decimal(os.getenv("SL_PERCENT", 0.0751))
    time_limit = Decimal(os.getenv("TIME_LIMIT", 60 * 5))

    trading_pairs = [pair for pair in trading_pairs.split(",")]
    candles = {pair: CandlesFactory.get_candle(CandlesConfig(connector='binance', trading_pair=pair, interval="1s", max_records=100)) for pair in trading_pairs}

    # Other Parameters
    markets = {
        exchange: set(trading_pairs)
    }
    stop_loss_dict = {}

    def __init__(self, connectors: Dict[str, ConnectorBase]):
        super().__init__(connectors)
        for candle in self.candles.values():
            candle.start()
        self.order_book_trade_event = SourceInfoEventForwarder(self._process_public_trade)
        self.trades_temp_storage = {trading_pair: [] for trading_pair in self.trading_pairs}
        self.subscribed_to_order_book_trade_event = False
        
        # Info creation
        self.all_buys_count = 0
        self.all_sells_count = 0
        self.losing_sell_count = 0
        self.trade_volume = 0
        self.initial_basic_price = None
        self.start_time = datetime.now()

    def on_tick(self):
        # self.log_with_clock(logging.INFO, f"Current Time: {datetime.utcfromtimestamp(self.current_timestamp).strftime('%d/%m/%Y %H:%M:%S')}")
        if not self.subscribed_to_order_book_trade_event:
            self.subscribe_to_order_book_trade_event()

        estimated_net_worth = self.estimate_net_worth()
        # self.log_with_clock(logging.INFO, f"Estimated Net Worth: {estimated_net_worth}")
        if estimated_net_worth < 2400:
            msg = f"Estimated Net Worth: {estimated_net_worth} too low, stopping the account to avoid any more losses"
            self.log_with_clock(logging.INFO, msg)
            self.notify_hb_app_with_timestamp(msg)
            HummingbotApplication.main_application().stop()

        # self.log_with_clock(logging.INFO, "Successfully subscribed to order book trade event")

        # self.log_with_clock(logging.INFO, f'My Active Orders: {self.active_orders}')

        if not self.initial_basic_price:
            self.initial_basic_price = self.market_conditions(self.exchange, 'BTC-FDUSD').get('mid_price')

        # Find Out My Account Balance
        account_balance = self.get_balance_df()
        # self.log_with_clock(logging.INFO, f'My Account Balance:\n{account_balance}')

        # Find Out My Open Orders
        limit_orders = self.get_active_orders(self.exchange)
        # self.log_with_clock(logging.INFO, f'My Limit Orders:\n{limit_orders}')
        

        for trading_pair in self.trading_pairs:
            # Return the best_ask, best_bid, and mid_price, and spread
            market = self.market_conditions(self.exchange, trading_pair)
            # self.log_with_clock(logging.INFO, f'{trading_pair} Market Conditions:\n{market_conditions}')


            # Find Out The Recent Completed Orders to determine if the price will move up or down

            # Find Out The Latest Order Book
            order_book = self.get_order_book_dict(self.exchange, trading_pair, self.depth)
            # self.log_with_clock(logging.INFO, f'{trading_pair} Order Book:\n{order_book}')

            # Get the candles for buying logic
            candle = self.candles[trading_pair]
            candle_df = candle.candles_df

            if not candle_df.empty:
                kdj_df = self.get_kdj_dataframe(candle_df=candle_df, period=9, ma_period_k=3, ma_period_d=3)
                kdj_buying_logic = kdj_df['J'].iloc[-1] > kdj_df['%D'].iloc[-1]
                # self.log_with_clock(logging.INFO, f'{trading_pair} Candles:\n{candle_df}')

                moving_averages = self.get_ema_dataframe(candle_df)
                ema_buying_logic = moving_averages['EMA_3'].iloc[-1] > moving_averages['EMA_7'].iloc[-1] > moving_averages['EMA_25'].iloc[-1]

                if len(moving_averages) > 1:
                    postitive_long_ema = moving_averages['EMA_35'].iloc[-1] > moving_averages['EMA_35'].iloc[-2]
                else:
                    postitive_long_ema = False

            else:
                kdj_buying_logic = False
                ema_buying_logic = False

            # Selling Logic
            if not limit_orders == None:
                self.triple_barrier_check(limit_orders, market.get('mid_price'))
                ongoing_limit_orders = len(limit_orders)
            else:
                ongoing_limit_orders = 0

            available_asset = account_balance.loc[account_balance['Asset'] == trading_pair.split('-')[1], 'Available Balance'].iloc[0]

            market_pressure = self.determine_market_pressure_from_completed_trades(self.trades_temp_storage[trading_pair])

            bid_pressure = self.determine_market_pressure(order_book=order_book)

            # Buying Logic
            self.log_with_clock(logging.INFO, f'''
                {trading_pair}
                market_pressure: {market_pressure}
                bid_pressure: {bid_pressure}
                kdj_buying_logic: {kdj_buying_logic}
                ema_buying_logic: {ema_buying_logic}
                positive_long_ema: {postitive_long_ema}
                ongoing_limit_orders: {ongoing_limit_orders}
                spread: ${market.get("spread"):,.2f}
                market_price: ${market.get("mid_price"):,.2f}
                available_asset: ${available_asset:,.2f}
            ''')
            if market_pressure == 'Buy_Pressure' and bid_pressure == 'Bid_Pressure' and kdj_buying_logic and ema_buying_logic and postitive_long_ema and market.get('spread') > self.spread_buy and ongoing_limit_orders <= self.max_orders and available_asset > 100:
                
                buying_power = available_asset * self.buying_percentage / 100
                amount_to_buy = Decimal(buying_power) / market.get('mid_price')
                
                self.buy(
                    connector_name=self.exchange,
                    trading_pair=trading_pair,
                    amount=amount_to_buy,
                    order_type=OrderType.MARKET
                )
    
    def market_conditions(self, exchange: str, trading_pair: str):
        best_ask = self.connectors[exchange].get_price(trading_pair, is_buy=True)
        best_bid = self.connectors[exchange].get_price(trading_pair, is_buy=False)
        mid_price = self.connectors[exchange].get_mid_price(trading_pair)
        spread = best_ask - best_bid
        return {
            'best_ask': best_ask,
            'best_bid': best_bid,
            'mid_price': mid_price,
            'spread': spread
        }
    
    def get_order_book_dict(self, exchange: str, trading_pair: str, depth: int = 50):
        order_book = self.connectors[exchange].get_order_book(trading_pair)
        snapshot = order_book.snapshot
        return {
            "timestamp": self.current_timestamp,
            "bids": snapshot[0].loc[:(depth - 1), ["price", "amount"]].values.tolist(),
            "asks": snapshot[1].loc[:(depth - 1), ["price", "amount"]].values.tolist(),
        }
    
    
    def determine_market_pressure(self, order_book):
        bids = order_book['bids']
        asks = order_book['asks']
    
        total_bid_volume = sum([volume for price, volume in bids])
        total_ask_volume = sum([volume for price, volume in asks])

        if total_bid_volume > total_ask_volume:
            return 'Bid_Pressure'
        elif total_bid_volume < total_ask_volume:
            return 'Ask_Pressure'
        else:
            return 'Equal_Pressure'
        
    def determine_market_pressure_from_completed_trades(self, trades):
        buy_volume = sum(trade['q_base'] for trade in trades if trade['side'] == 'buy')
        sell_volume = sum(trade['q_base'] for trade in trades if trade['side'] == 'sell')

        if buy_volume > sell_volume:
            return 'Buy_Pressure'
        elif sell_volume > buy_volume:
            return 'Sell_Pressure'
        else:
            return 'Equal_Pressure'

        
    def get_kdj_dataframe(self, candle_df, period=9, ma_period_k=3, ma_period_d=3):
        low_min = candle_df['low'].rolling(window=period, min_periods=1).min()
        high_max = candle_df['high'].rolling(window=period, min_periods=1).max()

        # Calculate %K
        candle_df['%K'] = ((candle_df['close'] - low_min) / (high_max - low_min)) * 100
        candle_df['%K'].fillna(0, inplace=True)  # Replace NaN values with 0 for the initial entries
        candle_df['%D'] = candle_df['%K'].rolling(window=ma_period_k, min_periods=1).mean()
        candle_df['J'] = 3 * candle_df['%K'] - 2 * candle_df['%D']

        return candle_df[['%K', '%D', 'J']]
    
    def get_ema_dataframe(self, candle_df):
        # Calculate EMAs for the specified periods
        candle_df['EMA_3'] = candle_df['close'].ewm(span=3, adjust=False).mean()
        candle_df['EMA_7'] = candle_df['close'].ewm(span=7, adjust=False).mean()
        candle_df['EMA_25'] = candle_df['close'].ewm(span=25, adjust=False).mean()
        candle_df['EMA_35'] = candle_df['close'].ewm(span=35, adjust=False).mean()
        # self.log_with_clock(logging.INFO, f"Candle DF: {candle_df.iloc[-1]}")

        # Return DataFrame with EMA columns
        return candle_df[['EMA_3', 'EMA_7', 'EMA_25', 'EMA_35']]

    def triple_barrier_check(self, limit_orders, mid_price):
        """
        Because the buy trade already has a limit order in it, then we just need to focus on checking the stop loss and time limit
        When either of these two occur, we will cancel the order, and then once the order has successfully cancelled we will sell at market price
        """
        for limit_order in limit_orders:
            # self.log_with_clock(logging.INFO, f"Limit Order: {limit_order} | Price: {limit_order.price} Bool: {limit_order.price < mid_price - self.stop_loss_percent} | Age: {limit_order.age()} | Bool: {limit_order.age() > self.time_limit}")
            if (limit_order.price < mid_price * (1 - self.stop_loss_percent / 100)) or (limit_order.age() > self.time_limit):
                self.log_with_clock(logging.INFO, f'Cancelling Order: {limit_order.client_order_id}')
                self.stop_loss_dict[limit_order.client_order_id] = limit_order.quantity
                self.cancel(self.exchange, limit_order.trading_pair, limit_order.client_order_id)

    def estimate_net_worth(self):
        balance_df = self.get_balance_df()
        estimated_net_worth = Decimal(0)
        for trading_pair in self.trading_pairs:
            base = trading_pair.split('-')[0]
            quote = trading_pair.split('-')[1]
            mid_price = Decimal(self.market_conditions(self.exchange, trading_pair).get('mid_price'))
            total_base = Decimal(balance_df.loc[balance_df['Asset'] == base, 'Total Balance'].values[0])
            total_quote = Decimal(balance_df.loc[balance_df['Asset'] == quote, 'Total Balance'].values[0])
            estimated_net_worth += Decimal((total_base * mid_price) + total_quote)
        
        return estimated_net_worth


    def did_create_buy_order(self, event: BuyOrderCreatedEvent):
        msg = (f"Created BUY order {event.order_id}")
        self.log_with_clock(logging.INFO, msg)
        # self.notify_hb_app_with_timestamp(msg)

    def did_create_sell_order(self, event: SellOrderCreatedEvent):
        msg = (f"Created SELL order {event.order_id}")
        self.log_with_clock(logging.INFO, msg)
        # self.notify_hb_app_with_timestamp(msg)

    def did_cancel_order(self, event: OrderCancelledEvent):
        trading_pair = self.pair
        amount = Decimal(self.stop_loss_dict.get(event.order_id, 'Error! Event Order ID not in stop_loss_dict'))

        msg = (f"Completed Cancel sell order {event.order_id} because it reached a stop loss or time limit | Trading Pair: {trading_pair} | Amount: {amount}")
        self.log_with_clock(logging.INFO, msg)

        self.sell(
            connector_name=self.exchange,
            trading_pair=trading_pair,
            amount=amount,
            order_type=OrderType.MARKET
        )
        self.losing_sell_count += 1
        self.stop_loss_dict.pop(event.order_id, 'Error! Event Order ID not in stop_loss_dict')

    def did_complete_buy_order(self, event: BuyOrderCompletedEvent):
        trading_pair = f'{event.base_asset}-{event.quote_asset}'
        amount = Decimal(event.base_asset_amount)
        bought_price = event.quote_asset_amount / event.base_asset_amount

        market_conditions = self.market_conditions(self.exchange, trading_pair)
        # spread = market_conditions.get('spread')

        sell_price = bought_price * (1 + self.take_profit_percent / 100)

        msg = (f"Completed BUY order {event.order_id} to buy {event.base_asset_amount} of {event.base_asset}. Trading Pair: {trading_pair}, amount: {amount}, price: {bought_price}, sell_price: {sell_price}")
        self.log_with_clock(logging.INFO, msg)
        # self.notify_hb_app_with_timestamp(msg)

        self.sell(
            connector_name=self.exchange,
            trading_pair=trading_pair,
            amount=amount,
            order_type=OrderType.LIMIT,
            price=Decimal(sell_price)
        )
        self.all_buys_count += 1
        self.trade_volume += amount * bought_price

    def did_complete_sell_order(self, event: SellOrderCompletedEvent):
        self.all_sells_count += 1
        msg = (f"Completed SELL order {event.order_id} to sell {event.base_asset_amount} of {event.base_asset}")
        self.log_with_clock(logging.INFO, msg)
        self.trade_volume += event.base_asset_amount * event.quote_asset_amount
        # self.notify_hb_app_with_timestamp(msg)

    def _process_public_trade(self, event_tag: int, market: ConnectorBase, event: OrderBookTradeEvent):
        self.trades_temp_storage[event.trading_pair].append({
            "ts": event.timestamp,
            "price": event.price,
            "q_base": event.amount,
            "side": event.type.name.lower(),
        })

    def subscribe_to_order_book_trade_event(self):
        for market in self.connectors.values():
            for order_book in market.order_books.values():
                order_book.add_listener(OrderBookEvent.TradeEvent, self.order_book_trade_event)
        self.subscribed_to_order_book_trade_event = True

    def time_elapsed(self, timedelta):
        days = timedelta.days
        hours, remainder = divmod(timedelta.seconds, 60*60)
        minutes, seconds = divmod(remainder, 60)
        return days, hours, minutes, seconds

    def format_status(self):
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        lines = []
        warning_lines = []
        warning_lines.extend(self.network_warning(self.get_market_trading_pair_tuples()))

        binance_time = datetime.utcfromtimestamp(self.current_timestamp).strftime('%d/%m/%Y %H:%M:%S')
        latest_time = datetime.now()

        time_difference = latest_time - self.start_time

        days, hours, minutes, seconds = self.time_elapsed(time_difference)

        lines.extend([f"Latest data at {binance_time} \n"])
        lines.extend([f"Been Trading for {days} Days {hours} Hours {minutes} Minutes {seconds} Seconds"])
        estimated_net_worth = self.estimate_net_worth()
        lines.extend([f"Estimated Net Worth: {estimated_net_worth}\n"])
        lines.extend([f"Buy Events Completed: {self.all_buys_count:,}"])
        lines.extend([f"Sell Events Completed: {self.all_sells_count:,}"])
        lines.extend([f"Of which {self.losing_sell_count:,} were non winners"])
        lines.extend([f"Total Trading Volume: ${self.trade_volume:,.2f}"])

        try:
            active_orders = self.active_orders_df()
            sell_orders = active_orders[active_orders['Side'] == 'sell']
            potential_trade = Decimal(sum(sell_orders['Price'] * sell_orders['Amount']))
        except:
            active_orders = pd.DataFrame()
            potential_trade = Decimal(0)
        
        balance_df = self.get_balance_df()

        potential_net_worth = Decimal(potential_trade)

        for trading_pair in self.trading_pairs:
            base = trading_pair.split('-')[0]
            quote = trading_pair.split('-')[1]
            base_price = Decimal(self.market_conditions(self.exchange, f'{base}-FDUSD').get('mid_price'))

            base_not_traded = Decimal(balance_df.loc[balance_df['Asset'] == base, 'Available Balance'].values[0])
            quote_not_traded = Decimal(balance_df.loc[balance_df['Asset'] == quote, 'Available Balance'].values[0])

            if not quote == 'FDUSD':
                quote_price = Decimal(self.market_conditions(self.exchange, f'{quote}-FDUSD').get('mid_price'))
                quote_val = quote_price * quote_not_traded
            else:
                quote_val = quote_not_traded

            
            trading_pair_value = base_price * base_not_traded + quote_val
            potential_net_worth += Decimal(trading_pair_value)

        lines.extend([f"Potential Net Worth: {potential_net_worth}\n"])

        basic_mid_price = Decimal(self.market_conditions(self.exchange, 'BTC-FDUSD').get('mid_price'))
        hold_strategy_percentage = basic_mid_price / self.initial_basic_price
        lines.extend([f"Hold Strategy of BTC-FDUSD Profit: ${hold_strategy_percentage * 2500:,.2f}"])

        lines.extend(["", "  Balances:"] + ["    " + line for line in balance_df.to_string(index=False).split("\n")])
        for trading_pair in self.trading_pairs:
            market_conditions = self.market_conditions(self.exchange, trading_pair)
            lines.extend([f"{trading_pair} Market Conditions: {market_conditions}\n"])

        if not active_orders.empty:
            lines.extend(["", "  Maker Orders:"] + ["    " + line for line in active_orders.to_string(index=False).split("\n")])
        else:
            lines.extend(["", "  No active maker orders."])

        warning_lines.extend(self.balance_warning(self.get_market_trading_pair_tuples()))
        if len(warning_lines) > 0:
            lines.extend(["", "*** WARNINGS ***"] + warning_lines)
        return "\n".join(lines)