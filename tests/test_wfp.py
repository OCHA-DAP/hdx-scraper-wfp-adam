#!/usr/bin/python
"""
Unit tests for WFP ADAM.

"""
from datetime import datetime, timezone
from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.vocabulary import Vocabulary
from hdx.location.country import Country
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent
from wfp import ADAM


class TestADAM:
    @pytest.fixture(scope="function")
    def fixtures(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="function")
    def configuration(self):
        Configuration._create(
            hdx_read_only=True,
            user_agent="test",
            project_config_yaml=join("config", "project_configuration.yml"),
        )
        UserAgent.set_global("test")
        Country.countriesdata(use_live=False)
        Locations.set_validlocations(
            [
                {"name": "fji", "title": "fji"},
                {"name": "mdg", "title": "mdg"},
            ]
        )
        configuration = Configuration.read()
        tags = (
            "geodata",
            "affected population",
            "earthquake-tsunami",
            "cyclones-hurricanes-typhoons",
        )
        Vocabulary._tags_dict = {tag: {"Action to Take": "ok"} for tag in tags}
        tags = [{"name": tag} for tag in tags]
        Vocabulary._approved_vocabulary = {
            "tags": tags,
            "id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            "name": "approved",
        }
        return configuration

    def test_generate_datasets_and_showcases(
        self,
        configuration,
        fixtures,
    ):
        with temp_dir(
            "test_wfp_adam", delete_on_success=True, delete_on_failure=False
        ) as folder:
            with Download() as downloader:
                retriever = Retrieve(downloader, folder, fixtures, folder, False, True)
                adam = ADAM(configuration, retriever)
                event_types = configuration["event_types"]
                all_events = []
                for event_type in event_types:
                    events = adam.parse_feed(
                        event_type, datetime(2020, 1, 1, 0, 0, tzinfo=timezone.utc)
                    )
                    all_events.extend(events)
                assert len(all_events) == 40

                dataset, showcases = adam.generate_dataset(all_events[3])
                assert dataset is None

                dataset, showcases = adam.generate_dataset(all_events[2])
                assert dataset == {
                    "name": "fiji-earthquake-6-6m-apr-2023",
                    "title": "Fiji: Earthquake - 6.6M - Apr 2023",
                    "notes": "A magnitude 6.6 earthquake at 562.474 depth occurred on Apr 18 2023 in South of the Fiji Islands. It impacted 0 people. The epicentre was at latitude -22.2844 longitude 179.3905.",
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "owner_org": "1ca198b6-e490-4cd0-9c1a-5b91bad9879a",
                    "data_update_frequency": "-1",
                    "subnational": "1",
                    "groups": [{"name": "fji"}],
                    "dataset_date": "[2023-04-18T00:00:00 TO 2023-04-18T00:00:00]",
                    "tags": [
                        {
                            "name": "affected population",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        }
                    ],
                    "customviz": [
                        {
                            "url": "https://mcarans.github.io/view-images/#https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_eq/events/2023/04/eq_us6000k587/eq_us6000k587.jpg"
                        }
                    ],
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "name": "sm_us6000k587_pop_estimation.xlsx",
                        "url": "https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_eq/events/2023/04/sm_us6000k587/sm_us6000k587_pop_estimation.xlsx",
                        "description": "Population Estimation",
                        "format": "xlsx",
                        "resource_type": "api",
                        "url_type": "api",
                    }
                ]
                assert showcases == [
                    {
                        "name": "fiji-earthquake-6-6m-apr-2023-shake-map-showcase",
                        "title": "Shake Map",
                        "notes": "Shake Map",
                        "url": "https://mcarans.github.io/view-images/#https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_eq/events/2023/04/sm_us6000k587/sm_us6000k587.jpg",
                        "image_url": "https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_eq/events/2023/04/sm_us6000k587/sm_us6000k587.jpg",
                        "tags": [
                            {
                                "name": "affected population",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            }
                        ],
                    }
                ]

                dataset, showcases = adam.generate_dataset(all_events[30])
                assert dataset == {
                    "name": "madagascar-cyclone-category-1-mar-2023",
                    "title": "Madagascar: Cyclone - Category 1 - Mar 2023",
                    "notes": "A cyclone (category 1) during the period Feb 06 2023-Mar 12 2023 in Mozambique, Madagascar. It impacted 1327678 people.",
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "owner_org": "1ca198b6-e490-4cd0-9c1a-5b91bad9879a",
                    "data_update_frequency": "-1",
                    "subnational": "1",
                    "groups": [{"name": "mdg"}],
                    "dataset_date": "[2023-02-06T00:00:00 TO 2023-03-12T23:59:59]",
                    "tags": [
                        {
                            "name": "geodata",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "affected population",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                    ],
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "name": "ADAM_TS_1000961_60_shp.zip",
                        "url": "https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_ts/events/2023/03/1000961_60/ADAM_TS_1000961_60_shp.zip",
                        "description": "Shape File",
                        "format": "shp",
                        "resource_type": "api",
                        "url_type": "api",
                    },
                    {
                        "name": "ADAM_TS_1000961_60_pop_estimation.xlsx",
                        "url": "https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_ts/events/2023/03/1000961_60/ADAM_TS_1000961_60_pop_estimation.xlsx",
                        "description": "Population Estimation",
                        "format": "xlsx",
                        "resource_type": "api",
                        "url_type": "api",
                    },
                ]
                assert showcases == [
                    {
                        "name": "madagascar-cyclone-category-1-mar-2023-wind-map-showcase",
                        "title": "Wind Map",
                        "notes": "Wind Map",
                        "url": "https://mcarans.github.io/view-images/#https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_ts/events/2023/03/1000961_60/adam_ts_1000961_60.jpg",
                        "image_url": "https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_ts/events/2023/03/1000961_60/adam_ts_1000961_60.jpg",
                        "tags": [
                            {
                                "name": "geodata",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            },
                            {
                                "name": "affected population",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            },
                        ],
                    },
                    {
                        "name": "madagascar-cyclone-category-1-mar-2023-rainfall-map-showcase",
                        "title": "Rainfall Map",
                        "notes": "Rainfall Map",
                        "url": "https://mcarans.github.io/view-images/#https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_ts/events/2023/03/1000961_60/adam_ts_1000961_60_rain5d.jpg",
                        "image_url": "https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_ts/events/2023/03/1000961_60/adam_ts_1000961_60_rain5d.jpg",
                        "tags": [
                            {
                                "name": "geodata",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            },
                            {
                                "name": "affected population",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            },
                        ],
                    },
                ]
