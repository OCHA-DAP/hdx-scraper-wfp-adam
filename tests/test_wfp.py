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
                    "name": "mex-earthquake-sm-us7000si51",
                    "notes": "**ADAM ID: sm\\_us7000si51**  Magnitude 5.7 earthquake at 22.194 "
                    "depth occurred on May 04 2026 2 km SE of Zocoteaca de León, Mexico. "
                    "The epicentre of the earthquake was located at latitude 16.6223 "
                    "longitude -97.9846. It impacted 271437 people within 50km.",
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
                    "title": "Mexico - Earthquake on May 04 2026",
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "dataset_preview_enabled": "False",
                        "description": "GeoJSON for earthquake on May 04 2026 2 km SE of Zocoteaca "
                        "de León, Mexico",
                        "format": "geojson",
                        "name": "mex_wfpadam_earthquake_20260504.geojson",
                    },
                    {
                        "dataset_preview_enabled": "False",
                        "description": "Shapefile for earthquake on May 04 2026 2 km SE of Zocoteaca "
                        "de León, Mexico",
                        "format": "shp",
                        "name": "mex_wfpadam_earthquake_20260504.zip",
                    },
                    {
                        "dataset_preview_enabled": "False",
                        "description": "Population estimation for earthquake on May 04 2026 2 km SE "
                        "of Zocoteaca de León, Mexico",
                        "format": "csv",
                        "name": "mex_wfpadam_earthquake_20260504.csv",
                    },
                ]
                assert showcases == [
                    {
                        "image_url": "https://static.gis.wfp.org/adam_eq/events/2026/05/sm_us7000si51/sm_us7000si51.jpg",
                        "name": "mex-earthquake-sm-us7000si51-map-showcase",
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
                    "dataset_preview": "no_preview",
                    "groups": [{"name": "png"}],
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "name": "png-storm-1001268",
                    "notes": "**ADAM ID: 1001268\\_18**  Tropical storm during the period Apr 04 "
                    "2026-Apr 10 2026 in Solomon Islands, Papua New Guinea. The center "
                    "of the storm was located near latitude -8.4 longitude 154.3.",
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
                    "title": "Papua New Guinea - Tropical storm on Apr 04 2026",
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "dataset_preview_enabled": "False",
                        "description": "Shapefile for Tropical storm during Apr 04 2026-Apr 10 2026 "
                        "in Solomon Islands, Papua New Guinea",
                        "format": "shp",
                        "name": "png_wfpadam_storm_20260404.zip",
                    },
                    {
                        "dataset_preview_enabled": "False",
                        "description": "Population estimation for Tropical storm during Apr 04 "
                        "2026-Apr 10 2026 in Solomon Islands, Papua New Guinea",
                        "format": "csv",
                        "name": "png_wfpadam_storm_20260404.csv",
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
                    "name": "mdg-flood-fl-20260213-mdg-00",
                    "notes": "**ADAM ID: FL-20260213-MDG-00**  A flood occurred on Feb 13 2026 in "
                    "Madagascar. It affected an estimated 1229989 people and 197440 ha "
                    "of cropland.",
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
                    "title": "Madagascar - Flood on Feb 13 2026",
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "dataset_preview_enabled": "False",
                        "description": "GeoTIFF for flood on Feb 13 2026 in Madagascar",
                        "format": "geotiff",
                        "name": "mdg_wfpadam_flood_20260213.zip",
                    },
                    {
                        "dataset_preview_enabled": "False",
                        "description": "Population estimation for flood on Feb 13 2026 in Madagascar",
                        "format": "xlsx",
                        "name": "mdg_wfpadam_flood_20260213.xlsx",
                    },
                ]
                assert showcases == [
                    {
                        "image_url": "https://multimedia.wfp.org/AssetLink/5c46vkj768qd0n6e12327eq14y857i8h.jpg",
                        "name": "mdg-flood-fl-20260213-mdg-00-flood-report-showcase",
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
