import logging

from join_engine import join_hash_partition


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    join_hash_partition(job_id="partitioned")


if __name__ == "__main__":
    main()
