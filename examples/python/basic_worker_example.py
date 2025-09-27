"""Basic example demonstrating the single-run Azolla Python worker."""

import asyncio
from dataclasses import dataclass

from azolla import Worker, WorkerInvocation, azolla_task


@azolla_task
async def greet_user(name: str, language: str = "en") -> dict[str, str]:
    """Return a localized greeting for the supplied user."""
    messages = {
        "en": "Hello",
        "es": "Hola",
        "fr": "Bonjour",
    }
    prefix = messages.get(language, messages["en"])
    return {"greeting": f"{prefix}, {name}!", "language": language}


# Manual task implementation using the Task trait
@dataclass
class AddNumbersTask:
    def name(self) -> str:
        return "add_numbers"

    async def execute(self, args):  # type: ignore[override]
        if not isinstance(args, list) or len(args) != 2:
            raise ValueError("add_numbers expects two positional arguments")
        return float(args[0]) + float(args[1])


async def main() -> None:
    # Build a worker with both decorated and manual tasks
    worker = (
        Worker.builder()
        .register_task(greet_user)
        .register_task(AddNumbersTask())
        .build()
    )

    print(f"Worker registered tasks: {worker.task_count()}")

    # Execute a task locally without going through the shepherd/orchestrator
    invocation = WorkerInvocation.from_json(
        task_id="demo-1",
        task_name="greet_user",
        args_json='["Alice"]',
        kwargs_json='{"language": "es"}',
        shepherd_endpoint="http://127.0.0.1:50052",  # unused for local execution
    )

    execution = await worker.execute(invocation)
    print("Invocation result:", execution.value)


if __name__ == "__main__":
    asyncio.run(main())
