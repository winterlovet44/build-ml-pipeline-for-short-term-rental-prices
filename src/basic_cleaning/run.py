#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    ######################
    df = pd.read_csv(artifact_local_path)

    min_price = args.min_price
    max_price = args.max_price

    logger.info("Remove outlier price in dataset...")
    df = df[df.price.between(min_price, max_price)].reset_index(drop=True)
    logger.info(f"Data contains {len(df)} rows with price from {min_price} to {max_price}")
    logger.info("Datatime data type normalizing...")
    dt_cols = 'last_review'
    df[dt_cols] = pd.to_datetime(df[dt_cols])
    logger.info(f"Convert column {dt_cols} to datetime datatype succeed.")

    df.to_csv("clean_sample.csv", index=False)
    # Save result to WanDB
    artifact = wandb.Artifact(
     args.output_artifact,
     type=args.output_type,
     description=args.output_description,
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)
    ######################


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Output artifact",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Data type of output",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Description of the output",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="lowest value of price",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="The highest value of price",
        required=True
    )


    args = parser.parse_args()

    go(args)
