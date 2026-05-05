#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""

import logging
from datetime import UTC, datetime
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.utilities.dateparse import now_utc
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

from hdx.scraper.wfp.adam._version import __version__
from hdx.scraper.wfp.adam.pipeline import Pipeline

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-wfp-adam"
updated_by_script = "HDX Scraper: WFP ADAM"


def main(save: bool = False, use_saved: bool = False) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to False.
        use_saved (bool): Use saved data. Defaults to False.

    Returns:
        None
    """

    logger.info(f"##### {lookup} version {__version__} ####")
    configuration = Configuration.read()
    User.check_current_user_write_access(
        "3ecac442-7fed-448d-8f78-b385ef6f84e7", configuration=configuration
    )

    with wheretostart_tempdir_batch(lookup) as info:
        folder = info["folder"]
        cur_state = datetime(2025, 1, 1, 0, 0, tzinfo=UTC)
        # with HDXState(
        #     "pipeline-state-wfp-adam",
        #     folder,
        #     parse_date,
        #     iso_string_from_datetime,
        #     configuration,
        # ) as state:
        with Download() as downloader:
            retriever = Retrieve(
                downloader, folder, "saved_data", folder, save, use_saved
            )
            today = now_utc()
            pipeline = Pipeline(configuration, retriever, today, folder)
            for event_type, eventtype_info in configuration["event_types"].items():
                #                    latest_episodes = pipeline.parse_feed(state.get(), eventtype_info)
                latest_episodes = pipeline.parse_feed(cur_state, eventtype_info)
                events = pipeline.parse_eventtypes_feed(latest_episodes, eventtype_info)
                logger.info(f"Number of datasets: {len(events)}")

                for event in events:
                    dataset, showcases = pipeline.generate_dataset(
                        latest_episodes, event
                    )
                    if not dataset:
                        continue
                    dataset.update_from_yaml(
                        script_dir_plus_file(
                            join("config", "hdx_dataset_static.yaml"), main
                        )
                    )
                    # ensure markdown has line breaks
                    dataset["notes"] = dataset["notes"].replace("\n", "  \n")

                    dataset.create_in_hdx(
                        remove_additional_resources=True,
                        updated_by_script=updated_by_script,
                        batch=info["batch"],
                    )
                    for showcase in showcases:
                        showcase.create_in_hdx()
                        showcase.add_dataset(dataset)
            # state.set(now_utc())


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
