import click


@click.version_option(prog_name="fluvii", package_name="fluvii")
@click.group()
def fluvii_cli():
    pass
