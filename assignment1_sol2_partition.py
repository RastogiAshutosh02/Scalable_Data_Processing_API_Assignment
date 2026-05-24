import logging

from join_engine import join_sqlite


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    join_sqlite(job_id="partitioned")


if __name__ == "__main__":
    main()
