"""
__main__.py:

Entry point of fprime-yamcs-dataproducts: the F Prime data product catalog for YAMCS. Launched by fprime-yamcs as a
YAMCS ProcessRunner service; run it directly when operating YAMCS without the launcher.

Licensed under the Apache License, Version 2.0 (the "License"); see http://www.apache.org/licenses/LICENSE-2.0
"""
import argparse
import logging
import os
import sys
import time
from pathlib import Path

from .logging import logger
from .processor import DEFAULT_IN_BUCKET, DEFAULT_OUT_BUCKET, FPrimeDataProductProcessor, default_work_dir


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="F Prime data product catalog for YAMCS: detects data products among downlinked files, "
                    "lists and decodes them for the F´ Data Products web page")
    parser.add_argument("--yamcs-url", default="http://localhost:8090",
                        help="YAMCS server URL (default: http://localhost:8090)")
    parser.add_argument("--instance", default=os.environ.get("FPRIME_YAMCS_INSTANCE", "fprime-project"),
                        help="YAMCS instance name (default: fprime-project)")
    parser.add_argument("--dictionary", type=Path, default=os.environ.get("FPRIME_DICTIONARY", None),
                        help="Path to the F Prime topology dictionary JSON (default: $FPRIME_DICTIONARY)")
    parser.add_argument("--in-bucket", default=DEFAULT_IN_BUCKET,
                        help=f"Bucket receiving downlinked files (default: {DEFAULT_IN_BUCKET})")
    parser.add_argument("--out-bucket", default=DEFAULT_OUT_BUCKET,
                        help=f"Bucket for the catalog and decoded JSON (default: {DEFAULT_OUT_BUCKET})")
    parser.add_argument("--work-dir", type=Path, default=None,
                        help=f"Local working directory (default: $FPRIME_DOWNLINK_DIR or {default_work_dir()})")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    args = parser.parse_args()
    if args.dictionary is None:
        parser.error("Supply --dictionary or set the FPRIME_DICTIONARY environment variable")
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    return args


def main():
    """Main entry point for the data product processor"""
    args = parse_args()
    try:
        processor = FPrimeDataProductProcessor(
            yamcs_url=args.yamcs_url,
            yamcs_instance=args.instance,
            dictionary_path=str(args.dictionary),
            in_bucket=args.in_bucket,
            out_bucket=args.out_bucket,
            work_dir=args.work_dir,
        )
        processor.start()
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        logger.error("Failed to start processor: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
