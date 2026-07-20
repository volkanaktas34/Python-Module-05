from typing import Protocol, List, Tuple
from ex0.data_processor import (
    NumericProcessor,
    TextProcessor,
    LogProcessor,
)
from ex1.data_stream import DataStream


class ExportPlugin(Protocol):

    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class CSVExport:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        if not data:
            return
        values = []
        for _, value in data:
            values.append(value)
        cvs = ", ".join(values)
        print(f"CSV Output:\n  {cvs}")


class JSONExport:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        if not data:
            return
        values = []
        for rank, value in data:
            values.append(f'"item_{rank}": "{value}"')
        json = "{" + ", ".join(values) + "}"
        print(f"JSON Output:\n  {json}")


class AdvancedDataStream(DataStream):
    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self._processors:
            collected_data: List[Tuple[int, str]] = []
            _, remaining = proc.get_stats()

            limit = min(nb, remaining)
            for _ in range(limit):
                try:
                    rank, val = proc.output()
                    collected_data.append((rank, val))
                except IndexError:
                    break
            if collected_data:
                plugin.process_output(collected_data)


if __name__ == "__main__":
    print("=== Code Nexus Data Pipeline ===")
    print("\nInitialize Data Stream...")
    pipeline = AdvancedDataStream()
    pipeline.print_processors_stats()

    print("\nRegistering Processors...")
    print(
        "\nSend first batch of data on stream: "
        "['Hello world', [3.14, -1, 2.71],"
        " [{'log_level': 'WARNING', 'log_message': "
        "'Telnet access! Use ssh instead'},"
        " {'log_level': 'INFO', 'log_message': 'User wil is connected'}],"
        " 42, ['Hi', 'five']]"
    )
    num_proc = NumericProcessor()
    text_proc = TextProcessor()
    log_proc = LogProcessor()
    pipeline.register_processor(num_proc)
    pipeline.register_processor(text_proc)
    pipeline.register_processor(log_proc)

    batch_1 = [
        'Hello world',
        [3.14, 1, 2.71],
        [{'log_level': 'WARNING',
          'log_message': 'Telnet access! Use ssh instead'},
         {'log_level': 'INFO',
          'log_message': 'User wil is connected'}],
        42,
        ['Hi', 'five']
    ]

    pipeline.process_stream(batch_1)
    pipeline.print_processors_stats()

    print("\nSend 3 processed data from each processor to a CSV plugin:")
    csv_plugin = CSVExport()
    pipeline.output_pipeline(3, csv_plugin)
    pipeline.print_processors_stats()

    print(
        "\nSend another batch of data: [21, ['I love AI', "
        "'LLMs are wonderful', 'Stay healthy'],"
        " [{'log_level': 'ERROR', 'log_message': '500 server crash'}, "
        "{'log_level': 'NOTICE',"
        " 'log_message': 'Certificate expires in 10 days'}], "
        "[32, 42, 64, 84, 128, 168], 'World hello']"
    )
    batch_2 = [
        21,
        ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
        [{'log_level': 'ERROR', 'log_message': '500 server crash'},
         {'log_level': 'NOTICE',
          'log_message': 'Certificate expires in 10 days'}],
        [32, 42, 64, 84, 128, 168],
        'World hello'
    ]

    pipeline.process_stream(batch_2)
    pipeline.print_processors_stats()

    print("\nSend 5 processed data from each processor to a JSON plugin:")
    json_plugin = JSONExport()
    pipeline.output_pipeline(5, json_plugin)
    pipeline.print_processors_stats()
