from typing import Any
from ex0.data_processor import (
    DataProcessor,
    NumericProcessor,
    TextProcessor,
    LogProcessor,
)


class DataStream():
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for element in stream:
            routed = False
            for proc in self._processors:
                if proc.validate(element):
                    try:
                        proc.ingest(element)
                        routed = True
                        break
                    except ValueError:
                        continue
            if not routed:
                print(f"DataStream error - "
                      f"Can't process element in stream: {element}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self._processors:
            print("No processor found, no data")
            return

        for proc in self._processors:
            name = proc.__class__.__name__.replace("Processor", " Processor")
            total, remaining = proc.get_stats()
            print(f"{name}: total {total} items processed, "
                  f"remaining {remaining} on processor")


if __name__ == "__main__":
    print("=== Code Nexus Data Stream ===")
    print("Initialize Data Stream...")
    stream_pipeline = DataStream()
    stream_pipeline.print_processors_stats()

    print("\nRegistering Numeric Processor")
    num_proc = NumericProcessor()
    stream_pipeline.register_processor(num_proc)

    batch = [
        'Hello world',
        [3.14, 1, 2.71],
        [{'log_level': 'WARNING',
          'log_message': 'Telnet access! Use ssh instead'},
         {'log_level': 'INFO', 'log_message': 'User wil is connected'}],
        42,
        ['Hi', 'five']
    ]

    print("Send first batch of data on stream...")
    stream_pipeline.process_stream(batch)
    stream_pipeline.print_processors_stats()

    print("\nRegistering other data processors")
    text_proc = TextProcessor()
    log_proc = LogProcessor()
    stream_pipeline.register_processor(text_proc)
    stream_pipeline.register_processor(log_proc)

    print("Send the same batch again")
    stream_pipeline.process_stream(batch)
    stream_pipeline.print_processors_stats()

    print(
        "\nConsume some elements from the data processors: "
        "Numeric 3, Text 2, Log 1"
    )
    for _ in range(3):
        num_proc.output()
    for _ in range(2):
        text_proc.output()
    for _ in range(1):
        log_proc.output()

    stream_pipeline.print_processors_stats()
