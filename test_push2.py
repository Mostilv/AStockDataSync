from src.data.cleaners import normalize_symbol, raw_symbol


def main() -> None:
    for value in ["600519", "000001", "BJ430047"]:
        print(value, "->", normalize_symbol(value), "->", raw_symbol(value))


if __name__ == "__main__":
    main()
