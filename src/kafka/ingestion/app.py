from app_builder import AppBuilder


def main():
    AppBuilder('ingestion-app') \
        .build() \
        .start()


if __name__ == '__main__':
    main()
