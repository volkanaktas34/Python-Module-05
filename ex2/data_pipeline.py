from abc import ABC, abstractmethod
from typing import Any, Protocol


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._storage: list[str] = []
        self._counter: int = 0
        self._total_processed: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self._storage:
            raise IndexError("No data remaining in processor.")
        data = self._storage.pop(0)
        rank = self._counter
        self._counter += 1
        return rank, data

    def get_stats(self) -> tuple[int, int]:
        return self._total_processed, len(self._storage)


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, (int, float)):
                    return False
            return True
        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        if isinstance(data, list):
            for item in data:
                self._storage.append(str(item))
                self._total_processed += 1
        else:
            self._storage.append(str(data))
            self._total_processed += 1


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, str):
                    return False
            return True
        return False

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")
        if isinstance(data, list):
            for item in data:
                self._storage.append(item)
                self._total_processed += 1
        else:
            self._storage.append(data)
            self._total_processed += 1


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            for key, value in data.items():
                if not isinstance(key, str):
                    return False
                if not isinstance(value, str):
                    return False
            return True
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, dict):
                    return False
                for key, value in item.items():
                    if not isinstance(key, str):
                        return False
                    if not isinstance(value, str):
                        return False
            return True
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")
        if isinstance(data, list):
            for item in data:
                entry = (
                    f"{item['log_level']}: "
                    f"{item['log_message']}"
                )
                self._storage.append(entry)
                self._total_processed += 1
        else:
            entry = (
                f"{data['log_level']}: "
                f"{data['log_message']}"
            )
            self._storage.append(entry)
            self._total_processed += 1


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class DataStream():
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for element in stream:
            processed = False
            for proc in self._processors:
                if proc.validate(element):
                    try:
                        proc.ingest(element)
                        processed = True
                        break
                    except ValueError:
                        continue
            if not processed:
                print(f"DataStream error - "
                      f"Can't process element in stream: {element}")

    def print_processors_stats(self) -> None:
        print("\n== DataStream statistics ==")
        if not self._processors:
            print("No processor found, no data")
            return
        for proc in self._processors:
            name = proc.__class__.__name__.replace("Processor", " Processor")
            total, remaining = proc.get_stats()
            print(f"{name}: total {total} items processed, "
                  f"remaining {remaining} on processor")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self._processors:
            data: list[tuple[int, str]] = []
            _, remaining = proc.get_stats()
            limit = min(nb, remaining)
            for _ in range(limit):
                try:
                    rank, value = proc.output()
                    data.append((rank, value))
                except IndexError:
                    break
            if data:
                plugin.process_output(data)


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


def main() -> None:
    print("=== Code Nexus Data Pipeline ===")
    print("\nInitialize Data Stream...")
    pipeline = DataStream()
    pipeline.print_processors_stats()

    print("\nRegistering Processors")
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

    data_1 = [
        'Hello world',
        [3.14, 1, 2.71],
        [{'log_level': 'WARNING',
          'log_message': 'Telnet access! Use ssh instead'},
         {'log_level': 'INFO',
          'log_message': 'User wil is connected'}],
        42,
        ['Hi', 'five']
    ]
    pipeline.process_stream(data_1)
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
    data_2 = [
        21,
        ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
        [{'log_level': 'ERROR', 'log_message': '500 server crash'},
         {'log_level': 'NOTICE',
          'log_message': 'Certificate expires in 10 days'}],
        [32, 42, 64, 84, 128, 168],
        'World hello'
    ]
    pipeline.process_stream(data_2)
    pipeline.print_processors_stats()

    print("\nSend 5 processed data from each processor to a JSON plugin:")
    json_plugin = JSONExport()
    pipeline.output_pipeline(5, json_plugin)
    pipeline.print_processors_stats()


if __name__ == "__main__":
    main()
