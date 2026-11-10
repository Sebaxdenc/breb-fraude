"""spark-submit /opt/breb/jobs/lakehouse.py <orden>: procesos batch de los DAG (breb.batch.cli)."""
import sys

from breb.batch.cli import main

sys.exit(main())
