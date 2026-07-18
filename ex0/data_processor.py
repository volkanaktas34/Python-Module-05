from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):

    def __init__(self) -> None:
        self._storage: list[str] = []
        self._processed: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self._storage:
            raise IndexError("No data remaining in processor.")

        rank = self._processed - len(self._storage)
        value = self._storage.pop(0)

        return rank, value


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

        if isinstance(data, (int, float)):
            self._storage.append(str(data))
            self._processed += 1
            return

        for item in data:
            self._storage.append(str(item))
            self._processed += 1


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

        if isinstance(data, str):
            self._storage.append(data)
            self._processed += 1
            return

        for item in data:
            self._storage.append(item)
            self._processed += 1


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

        if isinstance(data, dict):
            entry = (
                f"{data['log_level']}: "
                f"{data['log_message']}"
            )
            self._storage.append(entry)
            self._processed += 1
            return

        for item in data:
            entry = (
                f"{item['log_level']}: "
                f"{item['log_message']}"
            )
            self._storage.append(entry)
            self._processed += 1
