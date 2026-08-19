"""
oppna-insynsregistret: Open data pipeline for Swedish Finansinspektionen Insynsregister.
"""

from insyn.client import InsynClient, parse_export_csv
from insyn.updater import DatasetUpdater

__all__ = ["DatasetUpdater", "InsynClient", "parse_export_csv"]
__version__ = "0.1.0"
