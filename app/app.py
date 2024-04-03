import logging
import os
import threading
from typing import Iterable

from opentelemetry.metrics import CallbackOptions, Observation

from app import receiver
from app.variables import OTEL_SERVICE_NAME, METER

def send_number_of_threads(options: CallbackOptions) -> Iterable[Observation]:
    yield Observation(threading.active_count())


METER.create_observable_gauge(
    OTEL_SERVICE_NAME + ".wrt.gg.n.thr",
    callbacks=[send_number_of_threads],
    description="Number of Threads",
)

logging.basicConfig(level=logging.INFO)

receiver_socket = receiver.PETROSAReceiver(
    "binance_klines_current",
)

receiver_backfiller = receiver.PETROSAReceiver(
    "binance_sql_backfiller",
)

receiver_backfiller_binance = receiver.PETROSAReceiver(
    "binance_backfill",
)

logging.warning("Starting the writer")

th_receiver_socket = threading.Thread(
    target=receiver_socket.run, name="th_receiver_socket"
)
th_receiver_socket_bf = threading.Thread(
    target=receiver_backfiller.run, name="th_receiver_socket_bf"
)
th_receiver_socket_binancebackfill = threading.Thread(
    target=receiver_backfiller_binance.run, name="th_receiver_socket_binancebackfill"
)
th_receiver_socket.start()
th_receiver_socket_bf.start()
th_receiver_socket_binancebackfill.start()


while True:
    if th_receiver_socket.is_alive() == False:
        logging.warning("Restarting the th_receiver_socket")
        th_receiver_socket = threading.Thread(
            target=receiver_socket.run, name="th_receiver_socket"
        )
        th_receiver_socket.start()
    elif th_receiver_socket_bf.is_alive() == False:
        logging.warning("Restarting the th_receiver_socket_bf")
        th_receiver_socket_bf = threading.Thread(
            target=receiver_backfiller.run, name="th_receiver_socket_bf"
        )
        th_receiver_socket_bf.start()
    elif th_receiver_socket_binancebackfill.is_alive() == False:
        logging.warning("Restarting the th_receiver_socket_binancebackfill")
        th_receiver_socket_binancebackfill = threading.Thread(
            target=receiver_backfiller_binance.run,
            name="th_receiver_socket_binancebackfill",
        )
        th_receiver_socket_binancebackfill.start()
