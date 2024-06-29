from api.stream_api import StreamingApi


def main():
    # Run the Flink job
    print("Running Flink job")
    api = StreamingApi()
    api.prepare_stream()

    api.stream.print()
    # Execute the Flink job
    api.env.execute("Flink Streaming Job")


if __name__ == "__main__":
    main()
