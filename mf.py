#!/usr/bin/env python

"""
Batch metafield operations.

Requires the shopify package.
"""

# https://shopify.dev/docs/admin-api/rest/reference/metafield?api[version]=2020-07
# https://help.shopify.com/api/getting-started/authentication/private-authentication
# https://github.com/Shopify/shopify_python_api
# https://shopify.dev/concepts/about-apis/rate-limits

import time
import logging
import csv
import argparse
from configparser import ConfigParser
import shopify
import pyactiveresource.connection

CONFIG = ConfigParser()
CONFIG.read("config.ini")
CONFIG_MAIN = dict(CONFIG.items("main"))

LOGGER = logging.getLogger(__name__)
LOGLVL = 20
LOGGER.setLevel(LOGLVL)
LOGGER.addHandler(logging.StreamHandler())

def auth(store=None, api_key=None, password=None):
    """Set authentication details for shopify."""
    store = store or CONFIG_MAIN["store"]
    api_key = api_key or CONFIG_MAIN["api_key"]
    password = password or CONFIG_MAIN["password"]
    shop_url = "https://%s:%s@%s/admin" % (api_key, password, store)
    shopify.ShopifyResource.set_site(shop_url)
    LOGGER.info("auth: finished for store %s", store)

def get_mf_for(cls):
    """Get all metafield data for a given object class for the whole store."""
    mfs, objs_chunk = _get_mf_chunk(cls)
    log = lambda: LOGGER.info(
        "get_mf_for: %s: %d entries collected", cls.__name__, len(mfs))
    log()
    while objs_chunk.has_next_page():
        mfs_next, objs_chunk = _get_mf_chunk(cls, objs_chunk)
        mfs += mfs_next
        log()
    return mfs

def _get_mf_chunk(cls, objs_chunk=None):
    mfs = []
    if objs_chunk:
        objs_chunk = ftcall(objs_chunk.next_page)
    else:
        objs_chunk = ftcall(cls.find)
    for obj in objs_chunk:
        for mfdata in ftcall(obj.metafields):
            mfs.append(mfdata.to_dict())
    return mfs, objs_chunk

def ftcall(func):
    """Fault-tolerant API call wrapper."""
    LOGGER.debug("ftcall: %s", str(func))
    while True:
        try:
            try:
                return func()
            except pyactiveresource.connection.ClientError as err:
                headers = err.response.headers
                if "Retry-After" in headers:
                    retry = float(headers["Retry-After"]) * 2
                    LOGGER.debug("API rate limit reached; pausing %d secs", retry)
                    time.sleep(retry)
                else:
                    raise
        except pyactiveresource.connection.ServerError as err:
            LOGGER.error("Server error; continuing after 5 secs")
            time.sleep(5)

def metafields_csv(fp_out, classes=None):
    """Extract all metafield data to CSV.

    fp_out: file path for CSV output
    classes: string as space-separated list of API classes with metafields.
    """
    auth()
    classes = classes.split()
    mfs = []
    for cls in classes:
        mfs += get_mf_for(getattr(shopify, cls))
    with open(fp_out, "wt", newline="") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=mfs[0].keys(), lineterminator="\n")
        writer.writeheader()
        writer.writerows(mfs)

def configure_logging(quiet, verbose):
    """Adjust logging level

    quiet: integer by which to increment log level
    verbose: integer by wich to decrement log level
    """
    lvl = LOGLVL + 10 * (quiet - verbose)
    lvl = min(50, max(10, lvl))
    LOGGER.setLevel(lvl)

def main():
    """CLI for metafields_csv"""
    parser = argparse.ArgumentParser(
        description="Download Shopify metafields.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("csv", metavar="CSV", help="CSV file to save metafields to")
    parser.add_argument(
        "-c", "--classes",
        metavar="CLASSES",
        default="CustomCollection SmartCollection Product Variant",
        help="space-separated set of object classes")
    parser.add_argument("-v", action="count", default=0, help="increase logging verbosity")
    parser.add_argument("-q", action="count", default=0, help="decrease logging verbosity")
    args = parser.parse_args()
    configure_logging(args.q, args.v)
    metafields_csv(args.csv, args.classes)

if __name__ == "__main__":
    main()
