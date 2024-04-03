import logging
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Iterable
import retry
from petrosa.database import sql

from opentelemetry.metrics import CallbackOptions, Observation
from app.variables import (
    TRACER,
    SVC,
    METER,
    MAX_WORKERS,
    BATCH_SIZE,
    BATCH_TIME
)


class PETROSAWriter(object):
    @TRACER.start_as_current_span(name=SVC + ".wrt.init_writer")
    def __init__(self):
        self.queue = queue.Queue()
        self.executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

        self.sql_update_obs = 0

        self.candles_m5_list = []
        self.candles_m15_list = []
        self.candles_m30_list = []
        self.candles_h1_list = []

        METER.create_observable_gauge(
            SVC + ".write.sql.update.single",
            callbacks=[self.send_sql_update_obs],
            description="Time updating sql single doc",
        )

        METER.create_observable_gauge(
            SVC + ".write.gauge.queue.size",
            callbacks=[self.send_queue_size],
            description="Size of the queue for writing",
        )

        threading.Thread(target=self.update_forever).start()

    @TRACER.start_as_current_span(name=SVC + ".wrt.get_msg")
    def get_msg(self, table, msg):
        # print("msg on get_msg", msg)
        try:
            if msg["x"] is True:
                candle = {}
                candle["datetime"] = datetime.fromtimestamp(msg["t"] / 1000.0)
                candle["ticker"] = msg["s"]
                candle["open"] = float(msg["o"])
                candle["high"] = float(msg["h"])
                candle["low"] = float(msg["l"])
                candle["close"] = float(msg["c"])
                candle["closed_candle"] = msg["x"]

                if "T" in msg:
                    candle["close_time"] = datetime.fromtimestamp(msg["T"] / 1000.0)
                candle["insert_time"] = datetime.now(datetime.UTC)
                if "n" in msg:
                    candle["qty"] = float(msg["n"])
                if "q" in msg:
                    candle["quote_asset_volume"] = float(msg["q"])
                if "V" in msg:
                    candle["taker_buy_base_asset_volume"] = float(msg["V"])
                if "Q" in msg:
                    candle["taker_buy_quote_asset_volume"] = float(msg["Q"])
                if "n" in msg:
                    candle["vol"] = float(msg["n"])
                if "f" in msg:
                    candle["first_trade_id"] = msg["f"]
                if "L" in msg:
                    candle["last_trade_id"] = msg["L"]
                if "origin" in msg:
                    candle["origin"] = msg["origin"]
                candle["timestamp"] = int(time.time() * 1000)

                msg_table = {}
                msg_table["table"] = table
                msg_table["data"] = candle

                self.queue.put(msg_table)
        except Exception as e:
            print("Error in writer.py get_mesage()", e)
            pass

        self.last_data = time.time()

        return True

    def send_sql_update_obs(self, options: CallbackOptions) -> Iterable[Observation]:
        yield Observation(self.sql_update_obs)

    def send_queue_size(self, options: CallbackOptions) -> Iterable[Observation]:
        yield Observation(self.queue.qsize())

    @TRACER.start_as_current_span(name=SVC + ".wrt.update_forever")
    def update_forever(self):
        logging.info("Starting update_forever")

        last_m5 = time.time()
        last_m15 = time.time()
        last_m30 = time.time()
        last_h1 = time.time()

        while True:
            msg_table = self.queue.get()
            # print("message on writer", msg_table)

            if msg_table["table"] == "candles_m5":
                self.candles_m5_list.append(self.prepare_record(msg_table))
            elif msg_table["table"] == "candles_m15":
                self.candles_m15_list.append(self.prepare_record(msg_table))
            elif msg_table["table"] == "candles_m30":
                self.candles_m30_list.append(self.prepare_record(msg_table))
            elif msg_table["table"] == "candles_h1":
                self.candles_h1_list.append(self.prepare_record(msg_table))
            else:
                logging.error("weird stuff happening, table not recognized")
                logging.error(msg_table)

            try:
                if (
                    len(self.candles_m5_list) >= BATCH_SIZE
                    or ((time.time() - last_m5) >= BATCH_TIME and len(self.candles_m5_list) > 0)
                ):
                    self.update_sql(self.candles_m5_list, "candles_m5")
                    self.candles_m5_list.clear()
                    last_m5 = time.time()
                elif (
                    len(self.candles_m15_list) >= BATCH_SIZE
                    or( (time.time() - last_m15) >= BATCH_TIME and len(self.candles_m15_list) > 0)
                ):
                    self.update_sql(self.candles_m15_list, "candles_m15")
                    self.candles_m15_list.clear()
                    last_m15 = time.time()
                elif (
                    len(self.candles_m30_list) >= BATCH_SIZE
                    or ((time.time() - last_m30) >= BATCH_TIME and len(self.candles_m30_list) > 0)
                ):
                    self.update_sql(self.candles_m30_list, "candles_m30")
                    self.candles_m30_list.clear()
                    last_m30 = time.time()
                elif (
                    len(self.candles_h1_list) >= BATCH_SIZE
                    or ((time.time() - last_h1) >= BATCH_TIME and len(self.candles_h1_list) > 0)
                ):
                    self.update_sql(self.candles_h1_list, "candles_h1")
                    self.candles_h1_list.clear()
                    last_h1 = time.time()
            except Exception as e:
                logging.error("Error on update_forever", e)
                logging.info(msg_table)
                self.queue.put(msg_table)

    @TRACER.start_as_current_span(name=SVC + ".wrt.prepare_record")
    def prepare_record(self, record):
        record_prep = {}
        # record_prep["table"] = record["table"]
        record_prep["datetime"] = record["data"]["datetime"]
        record_prep["ticker"] = str(record["data"]["ticker"])
        record_prep["open"] = float(record["data"]["open"])
        record_prep["high"] = float(record["data"]["high"])
        record_prep["low"] = float(record["data"]["low"])
        record_prep["close"] = float(record["data"]["close"])
        record_prep["qty"] = float(record["data"]["qty"])
        record_prep["vol"] = float(record["data"]["vol"])
        record_prep["close_time"] = record["data"]["close_time"]
        record_prep["closed_candle"] = True
        if "origin" in record["data"]:
            record_prep["origin"] = record["data"]["origin"]
        else:
            record_prep["origin"] = "noorigin"
        record_prep["timestamp"] = int(time.time() * 1000)
        record_prep["insert_time"] = datetime.now(datetime.UTC)

        return record_prep

    @TRACER.start_as_current_span(name=SVC + ".wrt.update_sql")
    @retry.retry(tries=5, backoff=2, logger=logging.getLogger(__name__))
    def update_sql(self, candle_list, table, ):
        logging.info(f"Inserting {len(candle_list)} records on {table}")
        start_time = time.time_ns() // 1_000_000

        sql.update_sql(record_list=candle_list, table=table, mode="INSERT IGNORE")

        self.sql_update_obs = (time.time_ns() // 1_000_000) - start_time
