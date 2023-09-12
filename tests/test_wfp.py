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
from hdx.utilities.dateparse import parse_date
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
                {"name": "vut", "title": "vut"},
                {"name": "slv", "title": "slv"},
                {"name": "chn", "title": "chn"},
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
                today = parse_date("2023-07-27")
                adam = ADAM(configuration, retriever, today)
                adam.parse_feed(parse_date("2023-01-01"))
                adam.parse_eventtypes_feeds()
                events = adam.get_events()
                assert len(events) == 58

                dataset, showcases = adam.generate_dataset(events[0])
                assert dataset is None

                dataset, showcases = adam.generate_dataset(events[2])
                assert dataset == {
                    "customviz": [
                        {
                            "url": "https://mcarans.github.io/view-images/#https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_eq/events/2023/07/eq_us7000kgpb/eq_us7000kgpb.jpg"
                        }
                    ],
                    "data_update_frequency": "-1",
                    "dataset_date": "[2023-07-19T00:00:00 TO 2023-07-19T00:00:00]",
                    "groups": [{"name": "slv"}],
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "name": "el-salvador-earthquake-eq-us7000kgpb",
                    "notes": "A magnitude 6.5 earthquake at 69.727 depth occurred on Jul 19 2023 "
                    "in 43km S of Intipucá. It impacted 37502 people. The epicentre was "
                    "at latitude 12.814 longitude -88.1265.",
                    "owner_org": "1ca198b6-e490-4cd0-9c1a-5b91bad9879a",
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "affected population",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        }
                    ],
                    "title": "El Salvador: Earthquake - 6.5M - Jul 2023",
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "description": "Population Estimation",
                        "format": "xlsx",
                        "last_modified": "2023-07-19T00:22:07.000000",
                        "name": "sm_us7000kgpb_pop_estimation.xlsx",
                        "resource_type": "api",
                        "url": "https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_eq/events/2023/07/sm_us7000kgpb/sm_us7000kgpb_pop_estimation.xlsx",
                        "url_type": "api",
                    }
                ]
                assert showcases == [
                    {
                        'image_url': 'https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_eq/events/2023/07/eq_us7000kgpb/eq_us7000kgpb.jpg',
                        'name': 'el-salvador-earthquake-eq-us7000kgpb-map-showcase',
                        'notes': 'Map',
                        'tags': [{'name': 'affected population',
                                  'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}],
                        'title': 'Map',
                        'url': 'https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_eq/events/2023/07/eq_us7000kgpb/eq_us7000kgpb.jpg'},
                    {
                        "image_url": "https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_eq/events/2023/07/sm_us7000kgpb/sm_us7000kgpb.jpg",
                        "name": "el-salvador-earthquake-eq-us7000kgpb-shake-map-showcase",
                        "notes": "Shake Map",
                        "tags": [
                            {
                                "name": "affected population",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            }
                        ],
                        "title": "Shake Map",
                        "url": "https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_eq/events/2023/07/sm_us7000kgpb/sm_us7000kgpb.jpg",
                    }
                ]

                dataset, showcases = adam.generate_dataset(events[1])
                assert dataset == {
                    'customviz': [{
                                      'url': 'https://mcarans.github.io/view-images/#https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_ts/events/2023/07/1000985_28/adam_ts_1000985_28.jpg'}],
                    "data_update_frequency": "-1",
                    "dataset_date": "[2023-07-21T00:00:00 TO 2023-07-28T23:59:59]",
                    "groups": [{"name": "chn"}],
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "name": "china-cyclone-1000985",
                    "notes": "A cyclone (category 1) during the period Jul 21 2023-Jul 28 2023 in "
                    "China, Philippines. It impacted 23921068 people.",
                    "owner_org": "1ca198b6-e490-4cd0-9c1a-5b91bad9879a",
                    "subnational": "1",
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
                    "title": "China: Cyclone - Category 1 - Jul 2023",
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "description": "Shape File",
                        "format": "shp",
                        "last_modified": "2023-07-28T07:36:54.000000",
                        "name": "ADAM_TS_1000985_28_shp.zip",
                        "resource_type": "api",
                        "url": "https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_ts/events/2023/07/1000985_28/ADAM_TS_1000985_28_shp.zip",
                        "url_type": "api",
                    },
                    {
                        "description": "Population Estimation",
                        "format": "xlsx",
                        "last_modified": "2023-07-28T07:36:54.000000",
                        "name": "ADAM_TS_1000985_28_pop_estimation.xlsx",
                        "resource_type": "api",
                        "url": "https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_ts/events/2023/07/1000985_28/ADAM_TS_1000985_28_pop_estimation.xlsx",
                        "url_type": "api",
                    },
                ]
                assert showcases == [
                    {
                        "image_url": "https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_ts/events/2023/07/1000985_28/adam_ts_1000985_28.jpg",
                        "name": "china-cyclone-1000985-wind-map-showcase",
                        "notes": "Wind Map",
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
                        "title": "Wind Map",
                        "url": "https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_ts/events/2023/07/1000985_28/adam_ts_1000985_28.jpg",
                    },
                    {
                        "image_url": "https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_ts/events/2023/07/1000985_28/adam_ts_1000985_28_rain5d.jpg",
                        "name": "china-cyclone-1000985-rainfall-map-showcase",
                        "notes": "Rainfall Map",
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
                        "title": "Rainfall Map",
                        "url": "https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_ts/events/2023/07/1000985_28/adam_ts_1000985_28_rain5d.jpg",
                    },
                ]
