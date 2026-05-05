#!/usr/bin/python
"""
Unit tests for WFP ADAM.

"""

from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.wfp.adam.pipeline import Pipeline


class TestADAM:
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
                startdate = parse_date("2025-01-01")
                today = parse_date("2026-05-05")
                pipeline = Pipeline(configuration, retriever, today, folder)

                eventtype_info = configuration["event_types"]["SM"]
                latest_episodes = pipeline.parse_feed(startdate, eventtype_info)
                assert len(latest_episodes) == 1
                events = pipeline.parse_eventtypes_feed(latest_episodes, eventtype_info)
                assert len(events) == 1
                dataset, showcases = pipeline.generate_dataset(
                    latest_episodes, events[0]
                )
                assert dataset == {
                    "customviz": [
                        {
                            "url": "https://mcarans.github.io/view-images/#https://static.gis.wfp.org/adam_eq/events/2026/05/sm_us7000si51/sm_us7000si51.jpg"
                        }
                    ],
                    "data_update_frequency": "-1",
                    "dataset_date": "[2026-05-04T00:00:00 TO 2026-05-04T00:00:00]",
                    "dataset_preview": "no_preview",
                    "groups": [{"name": "mex"}],
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "name": "mexico-earthquake-sm-us7000si51",
                    "notes": "**ADAM ID: sm\\_us7000si51**  Magnitude 5.7 earthquake at "
                    "22.194 depth occurred on May 04 2026 2 km SE of Zocoteaca de León, Mexico. "
                    "The epicentre of the earthquake was located at latitude "
                    "16.6223 longitude -97.9846. It impacted 271437 people within 50km.",
                    "owner_org": "1ca198b6-e490-4cd0-9c1a-5b91bad9879a",
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "affected population",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "earthquake-tsunami",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "geodata",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                    ],
                    "title": "Mexico: Earthquake - 5.7M - May 2026",
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "dataset_preview_enabled": "False",
                        "description": "GeoJSON File",
                        "format": "geojson",
                        "name": "detail-us7000si51.geojson",
                    },
                    {
                        "dataset_preview_enabled": "False",
                        "description": "Shape File",
                        "format": "shp",
                        "name": "sm-us7000si51-sm-us7000si51-shp.zip",
                    },
                    {
                        "dataset_preview_enabled": "False",
                        "description": "Population Estimation",
                        "format": "csv",
                        "name": "sm-us7000si51-sm-us7000si51-pop-estimation.csv",
                    },
                ]
                assert showcases == [
                    {
                        "image_url": "https://static.gis.wfp.org/adam_eq/events/2026/05/sm_us7000si51/sm_us7000si51.jpg",
                        "name": "mexico-earthquake-sm-us7000si51-map-showcase",
                        "notes": "Map",
                        "tags": [
                            {
                                "name": "affected population",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            },
                            {
                                "name": "earthquake-tsunami",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            },
                            {
                                "name": "geodata",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            },
                        ],
                        "title": "Map",
                        "url": "https://static.gis.wfp.org/adam_eq/events/2026/05/sm_us7000si51/sm_us7000si51.jpg",
                    }
                ]

                eventtype_info = configuration["event_types"]["TS"]
                latest_episodes = pipeline.parse_feed(startdate, eventtype_info)
                assert len(latest_episodes) == 1
                events = pipeline.parse_eventtypes_feed(latest_episodes, eventtype_info)
                assert len(events) == 1
                dataset, showcases = pipeline.generate_dataset(
                    latest_episodes, events[0]
                )
                assert dataset == {
                    "data_update_frequency": "-1",
                    "dataset_date": "[2026-04-10T00:00:00 TO 2026-04-10T00:00:00]",
                    "dataset_preview": "resource_id",
                    "groups": [{"name": "png"}],
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "name": "papua-new-guinea-cyclone-1001268",
                    "notes": "**ADAM ID: 1001268\\_18**  Cyclone (tropical storm) during the "
                    "period Apr 04 2026-Apr 10 2026 in Solomon Islands, Papua New Guinea. "
                    "The center of the storm was located near latitude -8.4 longitude 154.3.",
                    "owner_org": "1ca198b6-e490-4cd0-9c1a-5b91bad9879a",
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "affected population",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "cyclones-hurricanes-typhoons",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "geodata",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                    ],
                    "title": "Papua New Guinea: Cyclone - Tropical storm - Apr 2026",
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "dataset_preview_enabled": "True",
                        "description": "Shape File",
                        "format": "shp",
                        "name": "1001268-18-adam-ts-1001268-18-shp.zip",
                    },
                    {
                        "dataset_preview_enabled": "False",
                        "description": "Population Estimation",
                        "format": "csv",
                        "name": "1001268-18-adam-ts-1001268-18-pop-estimation.csv",
                    },
                ]
                assert showcases == []

                eventtype_info = configuration["event_types"]["FL"]
                latest_episodes = pipeline.parse_feed(startdate, eventtype_info)
                assert len(latest_episodes) == 1
                events = pipeline.parse_eventtypes_feed(latest_episodes, eventtype_info)
                assert len(events) == 1
                dataset, showcases = pipeline.generate_dataset(
                    latest_episodes, events[0]
                )
                assert dataset == {
                    "data_update_frequency": "-1",
                    "dataset_date": "[2026-02-13T00:00:00 TO 2026-02-13T00:00:00]",
                    "dataset_preview": "no_preview",
                    "groups": [{"name": "mdg"}],
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "name": "madagascar-flood-fl-20260213-mdg-00",
                    "notes": "**ADAM ID: FL-20260213-MDG-00**  Flood event in Madagascar "
                    "effective Feb 13 2026. Estimated 1229989 people and "
                    "197440 ha of cropland affected.",
                    "owner_org": "1ca198b6-e490-4cd0-9c1a-5b91bad9879a",
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "affected population",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "flooding",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                        {
                            "name": "geodata",
                            "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                        },
                    ],
                    "title": "Madagascar: Flood - Feb 2026",
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "dataset_preview_enabled": "False",
                        "description": "Flood Extent Data",
                        "format": "shp",
                        "name": "02-fl-20260213-mdg-00.zip",
                    },
                    {
                        "dataset_preview_enabled": "False",
                        "description": "Population Estimation",
                        "format": "xlsx",
                        "name": "02-mdg-analysis-20260213-00.xlsx",
                    },
                ]
                assert showcases == [
                    {
                        "image_url": "https://multimedia.wfp.org/AssetLink/5c46vkj768qd0n6e12327eq14y857i8h.jpg",
                        "name": "madagascar-flood-fl-20260213-mdg-00-flood-report-showcase",
                        "notes": "Flood Report",
                        "tags": [
                            {
                                "name": "affected population",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            },
                            {
                                "name": "flooding",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            },
                            {
                                "name": "geodata",
                                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
                            },
                        ],
                        "title": "Flood Report",
                        "url": "https://static.gis.wfp.org/adam_fl/events/impact/2026/02/ADAM_MDG_FloodReport_20260213.pdf",
                    }
                ]
