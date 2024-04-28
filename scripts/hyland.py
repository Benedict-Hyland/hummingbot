import logging
from decimal import Decimal
from typing import Dict
import os
from datetime import datetime

from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.core.data_type.common import OrderType
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.core.event.events import OrderBookEvent, OrderBookTradeEvent
from hummingbot.data_feed.candles_feed.candles_factory import CandlesConfig, CandlesFactory
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.strategy.strategy_py_base import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
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
    trading_pairs = os.getenv("TRADING_PAIRS", "BTC-FDUSD")
    depth = int(os.getenv("DEPTH", 50))
    spread_factor = Decimal(os.getenv("SPREAD_FACTOR", 2.5))
    buying_percentage = os.getenv("BUYINGPERCENTAGE", 10)

    trading_pairs = [pair for pair in trading_pairs.split(",")]
    candles = CandlesFactory.get_candle(CandlesConfig(connector=exchange.split('_')[0], trading_pair='BTC-FDUSD', interval="1s", max_records=10))

    # Other Parameters
    markets = {
        exchange: set(trading_pairs)
    }

    def __init__(self, connectors: Dict[str, ConnectorBase]):
        super().__init__(connectors)
        self.candles.start()

    def on_tick(self):
        self.log_with_clock(logging.INFO, f"Current Time: {datetime.utcfromtimestamp(self.current_timestamp).strftime('%d/%m/%Y %H:%M:%S')}")

        # self.log_with_clock(logging.INFO, f'My Active Orders: {self.active_orders}')

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
            candle_df = self.candles.candles_df

            if not candle_df.empty:
                kdj_df = self.get_kdj_dataframe(candle_df=candle_df, period=9, ma_period_k=3, ma_period_d=3)
                kdj_buying_logic = kdj_df['J'].iloc[-1] > kdj_df['%D'].iloc[-1]
                # self.log_with_clock(logging.INFO, f'{trading_pair} Candles:\n{candle_df}')

                moving_averages = self.get_ema_dataframe(candle_df)
                ema_buying_logic = moving_averages['EMA_3'].iloc[-1] > moving_averages['EMA_7'].iloc[-1] > moving_averages['EMA_25'].iloc[-1]
            else:
                kdj_buying_logic = False
                ema_buying_logic = False

            available_asset = account_balance.loc[account_balance['Asset'] == trading_pair.split('-')[1], 'Available Balance'].iloc[0]

            # Buying Logic
            if self.determine_market_pressure(order_book=order_book) == 'Bid_Pressure' and kdj_buying_logic and ema_buying_logic and market.get('spread') > 1 and available_asset > 100:
                
                buying_power = available_asset * self.buying_percentage / 100
                amount_to_buy = Decimal(buying_power) / market.get('mid_price')
                
                self.buy(
                    connector_name=self.exchange,
                    trading_pair=trading_pair,
                    amount=Decimal(amount_to_buy),
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

        # Return DataFrame with EMA columns
        return candle_df[['EMA_3', 'EMA_7', 'EMA_25']]
    
    # @property
    # def all_candles_ready(self):
    #     """
    #     Checks if the candlesticks are full.
    #     :return:
    #     """
    #     return all([self.candles.ready])


    def did_create_buy_order(self, event: BuyOrderCreatedEvent):
        msg = (f"Created BUY order {event.order_id}")
        self.log_with_clock(logging.INFO, msg)
        # self.notify_hb_app_with_timestamp(msg)

    def did_create_sell_order(self, event: SellOrderCreatedEvent):
        msg = (f"Created SELL order {event.order_id}")
        self.log_with_clock(logging.INFO, msg)
        # self.notify_hb_app_with_timestamp(msg)

    def did_complete_buy_order(self, event: BuyOrderCompletedEvent):
        trading_pair = f'{event.base_asset}-{event.quote_asset}'
        amount = event.base_asset_amount
        price = event.quote_asset_amount / event.base_asset_amount

        market_conditions = self.market_conditions(self.exchange, trading_pair)
        spread = market_conditions.get('spread')

        sell_price = price + (spread / self.spread_factor)

        msg = (f"Completed BUY order {event.order_id} to buy {event.base_asset_amount} of {event.base_asset}. Trading Pair: {trading_pair}, amount: {amount}, price: {price}, sell_price: {sell_price}")
        self.log_with_clock(logging.INFO, msg)
        # self.notify_hb_app_with_timestamp(msg)

        self.sell(
            connector_name=self.exchange,
            trading_pair=trading_pair,
            amount=Decimal(amount),
            order_type=OrderType.LIMIT,
            price=Decimal(sell_price)
        )

    def did_complete_sell_order(self, event: SellOrderCompletedEvent):
        msg = (f"Completed SELL order {event.order_id} to sell {event.base_asset_amount} of {event.base_asset}")
        self.log_with_clock(logging.INFO, msg)
        # self.notify_hb_app_with_timestamp(msg)

    def format_status(self):
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        lines = []
        warning_lines = []
        warning_lines.extend(self.network_warning(self.get_market_trading_pair_tuples()))

        lines.extend([f"Latest data at {datetime.utcfromtimestamp(self.current_timestamp).strftime('%d/%m/%Y %H:%M:%S')} \n"])
        balance_df = self.get_balance_df()
        total_btc = Decimal(balance_df.loc[balance_df['Asset'] == 'BTC', 'Total Balance'].values[0])
        total_fdusd = Decimal(balance_df.loc[balance_df['Asset'] == 'FDUSD', 'Total Balance'].values[0])
        estimated_net_worth = total_btc * Decimal(self.market_conditions(self.exchange, 'BTC-FDUSD').get('mid_price')) + total_fdusd
        lines.extend([f"Estimated Net Worth: {estimated_net_worth}\n"])

        try:
            active_orders = self.active_orders_df()
        except:
            active_orders = []
        sell_orders = active_orders[active_orders['Side'] == 'sell']
        potential_trade = Decimal(sum(sell_orders['Price'] * sell_orders['Amount']))
        potential_net_worth = total_fdusd + potential_trade
        lines.extend([f"Potential Net Worth: {potential_net_worth}\n"])

        lines.extend(["", "  Balances:"] + ["    " + line for line in balance_df.to_string(index=False).split("\n")])
        for trading_pair in self.trading_pairs:
            market_conditions = self.market_conditions(self.exchange, trading_pair)
            lines.extend([f"{trading_pair} Market Conditions: {market_conditions}\n"])

        if len(active_orders) != 0:
            lines.extend(["", "  Maker Orders:"] + ["    " + line for line in active_orders.to_string(index=False).split("\n")])
        else:
            lines.extend(["", "  No active maker orders."])

        warning_lines.extend(self.balance_warning(self.get_market_trading_pair_tuples()))
        if len(warning_lines) > 0:
            lines.extend(["", "*** WARNINGS ***"] + warning_lines)
        return "\n".join(lines)