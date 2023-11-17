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
    def input_folder(self, fixtures):
        return join(fixtures, "input")

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
                {"name": "eth", "title": "eth"},
                {"name": "idn", "title": "idn"},
                {"name": "phl", "title": "phl"},
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
        input_folder,
    ):
        with temp_dir(
            "test_wfp_adam", delete_on_success=True, delete_on_failure=False
        ) as folder:
            with Download() as downloader:
                retriever = Retrieve(
                    downloader, folder, input_folder, folder, False, True
                )
                today = parse_date("2023-11-17")
                adam = ADAM(configuration, retriever, today, folder)
                adam.parse_feed(parse_date("2023-11-08"))
                adam.parse_eventtypes_feeds()
                events = adam.get_events()
                assert len(events) == 6

                dataset, showcases = adam.generate_dataset(events[0])
                assert dataset == {
                    "data_update_frequency": "-1",
                    "dataset_date": "[2023-11-14T00:00:00 TO 2023-11-14T00:00:00]",
                    "dataset_preview": "no_preview",
                    "groups": [{"name": "eth"}],
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "name": "ethiopia-flood-fl-20231114-eth-01",
                    "notes": "A flood covering 1016365.0 sq m on Nov 14 2023 in Ethiopia. It "
                    "impacted 534273 people.",
                    "owner_org": "1ca198b6-e490-4cd0-9c1a-5b91bad9879a",
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "geodata",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        }
                    ],
                    "title": "Ethiopia: Flood - 1016365.0 sq m - Nov 2023",
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "dataset_preview_enabled": "False",
                        "description": "Geopackage File",
                        "format": "geopackage",
                        "name": "FL-20231114-ETH-01.gpkg",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "dataset_preview_enabled": "False",
                        "description": "GeoJSON File",
                        "format": "geojson",
                        "name": "FL-20231114-ETH-01.geojson",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                    {
                        "dataset_preview_enabled": "False",
                        "description": "GeoTIFF File",
                        "format": "geotiff",
                        "name": "FL-20231114-ETH-01.tiff",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    },
                ]

                assert showcases == [
                    {
                        "image_url": "https://multimedia.wfp.org/AssetLink/5c46vkj768qd0n6e12327eq14y857i8h.jpg",
                        "name": "ethiopia-flood-fl-20231114-eth-01-report-showcase",
                        "notes": "Report",
                        "tags": [
                            {
                                "name": "geodata",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            }
                        ],
                        "title": "Report",
                        "url": "https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_fl/event/20231114/ADAM_ETH_FloodReport_20231114.pdf",
                    }
                ]

                dataset, showcases = adam.generate_dataset(events[4])
                assert dataset == {
                    "customviz": [
                        {
                            "url": "https://mcarans.github.io/view-images/#https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_eq/events/2023/11/eq_us7000l9ku/eq_us7000l9ku.jpg"
                        }
                    ],
                    "data_update_frequency": "-1",
                    "dataset_date": "[2023-11-08T00:00:00 TO 2023-11-08T00:00:00]",
                    "dataset_preview": "no_preview",
                    "groups": [{"name": "idn"}],
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "name": "indonesia-earthquake-eq-us7000l9ku",
                    "notes": "A magnitude 6.7 earthquake at 10.0 depth occurred on Nov 08 2023 in "
                    "Banda Sea. It impacted 0 people. The epicentre was at latitude "
                    "-6.1455 longitude 129.9137.",
                    "owner_org": "1ca198b6-e490-4cd0-9c1a-5b91bad9879a",
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "affected population",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        }
                    ],
                    "title": "Indonesia: Earthquake - 6.7M - Nov 2023",
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "dataset_preview_enabled": "False",
                        "description": "Population Estimation",
                        "format": "csv",
                        "name": "sm-us7000l9ku-sm-us7000l9ku-pop-estimation.csv",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    }
                ]
                assert showcases == [
                    {
                        "image_url": "https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_eq/events/2023/11/eq_us7000l9ku/eq_us7000l9ku.jpg",
                        "name": "indonesia-earthquake-eq-us7000l9ku-map-showcase",
                        "notes": "Map",
                        "tags": [
                            {
                                "name": "affected population",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            }
                        ],
                        "title": "Map",
                        "url": "https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_eq/events/2023/11/eq_us7000l9ku/eq_us7000l9ku.jpg",
                    },
                    {
                        "image_url": "https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_eq/events/2023/11/sm_us7000l9ku/sm_us7000l9ku.jpg",
                        "name": "indonesia-earthquake-eq-us7000l9ku-shake-map-showcase",
                        "notes": "Shake Map",
                        "tags": [
                            {
                                "name": "affected population",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            }
                        ],
                        "title": "Shake Map",
                        "url": "https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_eq/events/2023/11/sm_us7000l9ku/sm_us7000l9ku.jpg",
                    },
                ]

                dataset, showcases = adam.generate_dataset(events[2])
                assert dataset == {
                    "customviz": [
                        {
                            "url": "https://mcarans.github.io/view-images/#https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_ts/events/2023/11/1001032_6/adam_ts_1001032_6.jpg"
                        }
                    ],
                    "data_update_frequency": "-1",
                    "dataset_date": "[2023-11-12T00:00:00 TO 2023-11-13T23:59:59]",
                    "dataset_preview": "no_preview",
                    "groups": [{"name": "phl"}],
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "name": "philippines-cyclone-1001032",
                    "notes": "A cyclone (tropical depression) during the period Nov 12 2023-Nov "
                    "13 2023 in . It impacted 0 people.",
                    "owner_org": "1ca198b6-e490-4cd0-9c1a-5b91bad9879a",
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "geodata",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        }
                    ],
                    "title": "Philippines: Cyclone - Tropical depression - Nov 2023",
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "dataset_preview_enabled": "False",
                        "description": "Shape File",
                        "format": "shp",
                        "name": "1001032-6-adam-ts-1001032-6-shp.zip",
                        "resource_type": "file.upload",
                        "url_type": "upload",
                    }
                ]
                assert showcases == [
                    {
                        "image_url": "https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_ts/events/2023/11/1001032_6/adam_ts_1001032_6.jpg",
                        "name": "philippines-cyclone-1001032-wind-map-showcase",
                        "notes": "Wind Map",
                        "tags": [
                            {
                                "name": "geodata",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            }
                        ],
                        "title": "Wind Map",
                        "url": "https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_ts/events/2023/11/1001032_6/adam_ts_1001032_6.jpg",
                    },
                    {
                        "image_url": "https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_ts/events/2023/11/1001032_6/adam_ts_1001032_6_rain5d.jpg",
                        "name": "philippines-cyclone-1001032-rainfall-map-showcase",
                        "notes": "Rainfall Map",
                        "tags": [
                            {
                                "name": "geodata",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            }
                        ],
                        "title": "Rainfall Map",
                        "url": "https://adam-project-prod.s3-eu-west-1.amazonaws.com/adam_ts/events/2023/11/1001032_6/adam_ts_1001032_6_rain5d.jpg",
                    },
                ]
