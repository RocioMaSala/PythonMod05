from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._result: list[tuple[int, Any]] = []
        self._rank = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        return self._result.pop(0)


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            return all(isinstance(x, (int, float)) for x in data)
        return False

    def ingest(self, data: Any) -> None:
        try:
            if self.validate(data):
                if isinstance(data, list):
                    for x in data:
                        self._result.append((self._rank, str(x)))
                        self._rank += 1
                else:
                    self._result.append((self._rank, str(data)))
                    self._rank += 1
            else:
                raise ValueError(" - Got exception: Improper numeric data")
        except ValueError as e:
            print(e)

class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return all(isinstance(x, (str)) for x in data)
        return False

    def ingest(self, data: str | list[str]) -> None:
        if self.validate(data):
            if isinstance(data, list):
                for x in data:
                    self._result.append((self._rank, str(x)))
                    self._rank += 1
            else:
                self._result.append((self._rank, data))
                self._rank += 1
        else:
            raise ValueError("Got exception: Improper text data")


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            return True
        if isinstance(data, list):
            return all(isinstance(x, dict) for x in data)
        return False

    def ingest(self, data: dict[str, Any] | list[dict[str, Any]]) -> None:
        if self.validate(data):
            if isinstance(data, list):
                for x in data:
                    self._result.append((self._rank, x))  # echar un vistazo!!
                    self._rank += 1

            else:
                self._result.append((self._rank, str(data)))
                self._rank += 1
        else:
            raise ValueError("Got exception: Improper log data")


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===\n")

    print("Testing Numeric Processor...")
    np = NumericProcessor()
    num_1 = 42
    num_2 = "Hello"
    num_3 = "foo"
    num_4 = [1, 2, 3, 4, 5]

    print(f"Trying to validate input '{num_1}': {np.validate(num_1)}")
    print(f"Trying to validate input '{num_2}': {np.validate(num_2)}")

    print(
        f"Test invalid ingestion of string '{num_3}' without prior validation:"
        )
    np.ingest(num_3)

    print(f"Processing data: {num_4}")
    np.ingest(num_4)

    print("Extracting 3 values...")
    for d in range(3):
        rank, data = np.output()
        print(f"Numeric value {rank}: {data}")

    print("\nTesting Text Processor...")
    tp = TextProcessor()
    txt_1 = 42
    txt_2 = ["Hello", "Nexus", "World"]

    print(f"Trying to validate input '{txt_1}': {tp.validate(txt_1)}")

    print(f"Processing data: {txt_2}")
    tp.ingest(txt_2)

    print("Extracting 1 value... ")
    rank, data = tp.output()
    print(f"Text value {rank}: {data}")

    print("\nTesting Log Processor...")
    lp = LogProcessor()
    log_1 = "Hello"
    log_2 = [
        {"log_level": "NOTICE", "log_message": "Connection to server"},
        {"log_level": "ERROR", "log_message": "Unauthorized access!!"}
        ]

    print(f"Trying to validate input '{log_1}': {lp.validate(log_1)}")

    print(f"Processing data: {log_2}")
    lp.ingest(log_2)

    print("Extracting 2 values... ")
    for d in range(2):
        rank, data = lp.output()
        print(f"Log entry {rank}: {data['log_level']}: {data['log_message']}")
