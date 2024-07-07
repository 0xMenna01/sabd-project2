from stream_emulator.emulator import StreamEmulator
import optparse


def main():
    # i want an optional --fast flag to be able to run the stream emulator in fast mode
    parser = optparse.OptionParser()
    parser.add_option(
        "--fast",
        action="store_true",
        dest="fast",
        default=False,
        help="Run the stream emulator in fast mode.",
    )
    options, args = parser.parse_args()

    StreamEmulator(is_fast=options.fast).start()


if __name__ == "__main__":
    main()
