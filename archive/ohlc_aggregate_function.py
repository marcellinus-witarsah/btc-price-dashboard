
from pyflink.table import AggregateFunction, DataTypes
from pyflink.common import Row

class OHLCAggregateFunction(AggregateFunction):
    def create_accumulator(self):
        # open, high, low, close, first_ts, last_ts
        return Row(None, float("-inf"), float("inf"), None, None, None)

    def get_accumulator_type(self):
        return DataTypes.ROW([
            DataTypes.FIELD("open_price", DataTypes.FLOAT()),
            DataTypes.FIELD("high_price", DataTypes.FLOAT()),
            DataTypes.FIELD("low_price", DataTypes.FLOAT()),
            DataTypes.FIELD("close_price", DataTypes.FLOAT()),
            DataTypes.FIELD("first_ts", DataTypes.BIGINT()),
            DataTypes.FIELD("last_ts", DataTypes.BIGINT())
        ])
        
    def accumulate(self, accumulator, price, ts):
        # ts expected to be BIGINT (epoch millis) for 
        if accumulator[0] is None or ts < accumulator[4] or accumulator[4] is None:
            accumulator[0] = price
            accumulator[4] = ts

        if accumulator[3] is None or ts >= accumulator[5] or accumulator[5] is None:
            accumulator[3] = price
            accumulator[5] = ts

        accumulator[1] = max(accumulator[1], price)
        accumulator[2] = min(accumulator[2], price)

    def get_result_type(self):
        return DataTypes.ROW([
            DataTypes.FIELD("open_price", DataTypes.FLOAT()),
            DataTypes.FIELD("high_price", DataTypes.FLOAT()),
            DataTypes.FIELD("low_price", DataTypes.FLOAT()),
            DataTypes.FIELD("close_price", DataTypes.FLOAT())
        ])

    def get_value(self, accumulator):
        return Row(accumulator[0],accumulator[1],accumulator[2],accumulator[3])