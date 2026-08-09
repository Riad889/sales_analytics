from pathlib import Path

from importlib import import_module
from argparse import ArgumentParser

BASE_DIR = Path(__file__).resolve().parent
breakpoint()


def main():
    parser = ArgumentParser(description="Generate booking data")
    parser.add_argument(
        "--data_size",
        type=int,
        default=100_00_000,
        help="Number of booking records to generate (default: 100,000,000)",
    )
    parser.add_argument(
        "--chunk_size",
        type=int,
        default=100_000,
        help="Number of booking records to generate per chunk (default: 100,000)",
    )

    parser.add_argument(
        "--data_generator", choices=["booking"], help="Type of data generator to use"
    )

    args = parser.parse_args()

    if args.data_generator:
        module_name = f"generators.{args.data_generator}_generator"

        generator_module = import_module(module_name)
        class_name = f"{args.data_generator.capitalize()}Generator"
        generator_class = getattr(generator_module, class_name)
        generator = generator_class(
            data_size=args.data_size, chunk_size=args.chunk_size
        )
        filepath = BASE_DIR / f"data/{args.data_generator}_data.jsonl"
        filepath.parent.mkdir(parents=True, exist_ok=True)

        for chunk in generator.generate_data():
            generator.shared_utils.save_to_jsonl(chunk, filepath)


if __name__ == "__main__":
    main()
