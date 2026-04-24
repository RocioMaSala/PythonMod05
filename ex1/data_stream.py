from abc import ABC, abstractmethod
from typing import Any
import typing


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
                raise ValueError("Got exception: Improper numeric data")
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
        try:
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
        except ValueError as e:
            print(e)


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            return True
        if isinstance(data, list):
            return all(isinstance(x, dict) for x in data)
        return False

    def ingest(self, data: dict[str, Any] | list[dict[str, Any]]) -> None:
        try:
            if self.validate(data):
                if isinstance(data, list):
                    for x in data:
                        formatted = "|".join(f"{k}={v}" for k, v in x.items())
                        self._result.append((self._rank, formatted))
                        self._rank += 1

                else:
                    formatted = "|".join(f"{k}={v}" for k, v in x.items())
                    self._result.append((self._rank, formatted))
                    self._rank += 1
            else:
                raise ValueError("Got exception: Improper log data")
        except ValueError as e:
            print(e)


class DataStream:
    def __init__(self) -> None:
        print("Initialize Data Stream...")
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        if proc:
            self.processors.append(proc)
        else:
            return

    def process_stream(self, stream: list[typing.Any]) -> None:
        for data in stream:
            processed = False
            for processor in self.processors:
                if processor.validate(data):
                    processor.ingest(data)
                    processed = True
            if not processed:
                print(
                 f"Data Stream error - Can't process element in stream {data}"
                )

    def print_processors_stats(self) -> None:
        print("=== DataStream statistics ===")
        if not self.processors:
            print("No processor found, no data")
        else:
            for processor in self.processors:
                name = processor.__class__.__name__
                total = processor._rank
                remaining = len(processor._result)
                print(
                    f"{name}: total {total} items processed, "
                    f"remaining {remaining} on processor"
                    )


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===\n")

    dt = DataStream()
    dt.print_processors_stats()
    print()

    np = NumericProcessor()
    dt.register_processor(np)

    print("Registering Numeric Processor\n")
    stream = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {
                "log_level": "WARNING",
                "log_message": "Telnet access! Use ssh instead"
            },
            {
                "log_level": "INFO",
                "log_message": "User wil is connected"
            }
        ],
        42,
        ["Hi", "five"]
        ]
    print(f"Send first batch of data on stream: {stream}")
    dt.process_stream(stream)

    print()
    dt.print_processors_stats()

    print()
    print("Registering other data processors")
    tp = TextProcessor()
    lp = LogProcessor()
    dt.register_processor(tp)
    dt.register_processor(lp)
    print("Send the same batch again")
    dt.process_stream(stream)

    dt.print_processors_stats()

    print(
        "\nConsume some elements from the data processsors: "
        "Numeric 3, Text 2, Log 1"
        )
    for _ in range(3):
        rank, value = np.output()
    for _ in range(2):
        rank, value = tp.output()
    for _ in range(1):
        rank, value = lp.output()

    dt.print_processors_stats()
