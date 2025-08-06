"""Command-line interface."""
import click


@click.command()
@click.version_option()
def main() -> None:
    """Test Skatt Dqv."""


if __name__ == "__main__":
    main(prog_name="test-skatt-dqv")  # pragma: no cover
