import write_utils
from consumer import Consumer


def main():
    write_utils.init_env()
    Consumer(name="query-results-consumer").consume()


if __name__ == "__main__":
    main()
