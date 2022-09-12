# -*- coding: utf-8 -*-
# Copyright: (c) 2021, XLAB Steampunk <steampunk@xlab.si>
#
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function
from operator import inv

__metaclass__ = type

import sys

import pytest

from ansible.errors import AnsibleParserError, AnsibleError
from ansible.inventory.data import InventoryData
from ansible.module_utils.common.text.converters import to_text
from ansible.template import Templar

from ansible_collections.servicenow.itsm.plugins.inventory import now
from ansible_collections.servicenow.itsm.plugins.module_utils.table import TableClient

pytestmark = pytest.mark.skipif(
    sys.version_info < (2, 7), reason="requires python2.7 or higher"
)


@pytest.fixture
def inventory_plugin():
    plugin = now.InventoryModule()
    plugin.inventory = InventoryData()
    plugin.templar = Templar(loader=None)
    return plugin


class TestContructSysparmQuery:
    def test_valid_query(self):
        assert "column=value" == now.construct_sysparm_query(
            [dict(column="= value")], False
        )

    def test_invalid_query(self):
        with pytest.raises(AnsibleParserError, match="INVALID"):
            now.construct_sysparm_query([dict(column="INVALID operator")], False)

    def test_valid_encoded_query(self):
        assert "column=value^ORfield=something" == now.construct_sysparm_query(
            "column=value^ORfield=something", True
        )


class TestFetchRecords:
    def test_no_query(self, table_client):
        now.fetch_records(table_client, "table_name", None)

        table_client.list_records.assert_called_once_with(
            "table_name", dict(sysparm_display_value=True)
        )

    def test_query(self, table_client):
        now.fetch_records(table_client, "table_name", [dict(my="!= value")])

        table_client.list_records.assert_called_once_with(
            "table_name", dict(sysparm_display_value=True, sysparm_query="my!=value")
        )

    def test_no_query_with_fields(self, table_client):
        now.fetch_records(table_client, "table_name", None, fields=["a", "b", "c"])

        table_client.list_records.assert_called_once_with(
            "table_name", dict(sysparm_display_value=True, sysparm_fields="a,b,c")
        )


class TestInventoryModuleVerifyFile:
    @pytest.mark.parametrize(
        "name,valid",
        [("sample.now.yaml", True), ("sample.now.yml", True), ("invalid.yaml", False)],
    )
    def test_file_name(self, inventory_plugin, tmp_path, name, valid):
        config = tmp_path / name
        config.write_text(to_text("plugin: servicenow.itsm.now"))

        assert inventory_plugin.verify_file(to_text(config)) is valid


class TestInventoryModuleAddHost:
    def test_valid(self, inventory_plugin):
        host = inventory_plugin.add_host(
            dict(name_source="dummy_host", sys_id="123"),
            "name_source",
        )

        assert host == "dummy_host"
        hostvars = inventory_plugin.inventory.get_host("dummy_host").vars
        assert hostvars is not None

    def test_valid_empty_name(self, inventory_plugin):
        host = inventory_plugin.add_host(
            dict(name_source="", sys_id="123"),
            "name_source",
        )

        assert host is None
        assert inventory_plugin.inventory.get_host("dummy_host") is None

    def test_invalid_name(self, inventory_plugin):
        with pytest.raises(AnsibleParserError, match="invalid_name"):
            inventory_plugin.add_host(
                dict(name_source="dummy_host", sys_id="123"),
                "invalid_name",
            )


class TestInventoryModuleSetHostvars:
    def test_valid(self, inventory_plugin):
        inventory_plugin.inventory.add_host("dummy_host")

        inventory_plugin.set_hostvars(
            "dummy_host",
            dict(sys_id="123", platform="demo", unused="column"),
            ("sys_id", "platform"),
        )

        hostvars = inventory_plugin.inventory.get_host("dummy_host").vars
        assert hostvars["sys_id"] == "123"
        assert hostvars["platform"] == "demo"
        assert "unused" not in hostvars

    def test_invalid_column(self, inventory_plugin):
        with pytest.raises(AnsibleParserError, match="bad_column"):
            inventory_plugin.set_hostvars(
                "dummy_host",
                dict(sys_id="123", platform="demo", unused="column"),
                ("sys_id", "platform", "bad_column"),
            )


class TestInstance:
    @pytest.mark.parametrize(
        "instance_conf,instance_env,expected",
        [
            (dict(), dict(), dict()),
            (dict(a="a"), dict(), dict()),
            (dict(), dict(a="a"), dict(a="a")),
            (dict(a="a", b="b"), dict(a="c"), dict(a="a")),
            (dict(a="a"), dict(a="c", b="b"), dict(a="a", b="b")),
        ],
    )
    def test_merge_instance_config(
        self, inventory_plugin, instance_conf, instance_env, expected
    ):
        merged_conf = inventory_plugin._merge_instance_config(
            instance_conf, instance_env
        )

        assert merged_conf == expected

    def test_get_instance_from_env(self, inventory_plugin, mocker):
        def getenv(key):
            return dict(
                SN_HOST="host",
                SN_USERNAME="username",
                SN_PASSWORD="password",
                SN_CLIENT_ID="client_id",
                SN_SECRET_ID="client_secret",
                SN_REFRESH_TOKEN="refresh_token",
                SN_GRANT_TYPE="grant_type",
                SN_TIMEOUT="timeout",
            ).get(key)

        mocker.patch("os.getenv", new=getenv)

        config = inventory_plugin._get_instance_from_env()
        assert config == dict(
            host="host",
            username="username",
            password="password",
            client_id="client_id",
            client_secret="client_secret",
            refresh_token="refresh_token",
            grant_type="grant_type",
            timeout="timeout",
        )

    def test_get_instance(self, inventory_plugin, mocker):
        def get_option(*args):
            return dict(a="a", password="b", host="host")

        mocker.patch("os.getenv", new=lambda x: x)
        mocker.patch.object(inventory_plugin, "get_option", new=get_option)

        instance = inventory_plugin._get_instance()

        assert instance == dict(
            host="host",
            username="SN_USERNAME",
            password="b",
            client_id="SN_CLIENT_ID",
            client_secret="SN_SECRET_ID",
            refresh_token="SN_REFRESH_TOKEN",
            grant_type="SN_GRANT_TYPE",
            timeout="SN_TIMEOUT",
        )


class TestGetAndValidateOptions:
    def test_get_and_validate_options_query(self, inventory_plugin, mocker):
        def get_option(key):
            if key == "sysparm_query":
                return None
            return key

        mocker.patch.object(inventory_plugin, "get_option", new=get_option)

        inventory_plugin._get_and_validate_options()

        assert inventory_plugin._sn_enhanced == "enhanced"
        assert inventory_plugin._sn_table == "table"
        assert inventory_plugin._sn_name_source == "inventory_hostname_source"
        assert inventory_plugin._sn_columns == "columns"
        assert inventory_plugin._sn_compose == "compose"
        assert inventory_plugin._sn_groups == "groups"
        assert inventory_plugin._sn_keyed_groups == "keyed_groups"
        assert inventory_plugin._sn_strict == "strict"
        assert inventory_plugin._sn_cache == "cache"
        assert inventory_plugin._sn_query == "query"
        assert inventory_plugin._is_encoded_query is False

    def test_get_and_validate_sysparm_query(self, inventory_plugin, mocker):
        def get_option(key):
            if key == "query":
                return None
            return key

        mocker.patch.object(inventory_plugin, "get_option", new=get_option)

        inventory_plugin._get_and_validate_options()

        assert inventory_plugin._sn_enhanced == "enhanced"
        assert inventory_plugin._sn_table == "table"
        assert inventory_plugin._sn_name_source == "inventory_hostname_source"
        assert inventory_plugin._sn_columns == "columns"
        assert inventory_plugin._sn_compose == "compose"
        assert inventory_plugin._sn_groups == "groups"
        assert inventory_plugin._sn_keyed_groups == "keyed_groups"
        assert inventory_plugin._sn_strict == "strict"
        assert inventory_plugin._sn_cache == "cache"
        assert inventory_plugin._sn_query == "sysparm_query"
        assert inventory_plugin._is_encoded_query is True

    def test_get_and_validate_options_both_queries(self, inventory_plugin, mocker):
        def get_option(key):
            return key

        mocker.patch.object(inventory_plugin, "get_option", new=get_option)

        with pytest.raises(AnsibleParserError, match="mutually exclusive"):
            inventory_plugin._get_and_validate_options()


class TestMakeTableClient:
    def test_make_table_client(self, inventory_plugin, mocker):
        def get_instance():
            return dict(
                host="https://host.com",
                username="username",
                password="password",
                grant_type="grant_type",
                refresh_token="refresh_token",
                client_id="client_id",
                client_secret="client_secret",
                timeout="timeout",
            )

        mocker.patch.object(inventory_plugin, "_get_instance", new=get_instance)

        inventory_plugin._make_table_client()

        assert isinstance(inventory_plugin._sn_table_client, TableClient)

    def test_make_table_client_fail(self, inventory_plugin, mocker):
        def get_instance():
            return dict(
                host="host",
            )

        mocker.patch.object(inventory_plugin, "_get_instance", new=get_instance)

        with pytest.raises(AnsibleParserError, match="host"):
            inventory_plugin._make_table_client()


class TestInventoryModuleCache:
    @pytest.mark.parametrize(
        "cache_key,expected",
        [
            ("path1", [dict(a="a", b="b")]),
            ("path2", [dict(a="a", c="c")]),
            ("path3", None),
        ],
    )
    def test_get_cached_records(self, inventory_plugin, mocker, cache_key, expected):
        cache = dict(
            path1=[dict(a="a", b="b")],
            path2=[dict(a="a", c="c")],
        )

        mocker.patch.object(inventory_plugin, "_cache", new=cache)

        records = inventory_plugin._get_cached_records(cache_key)

        assert records == expected

    def test_update_cache(self, inventory_plugin, mocker):
        mocker.patch.object(inventory_plugin, "_cache", new=dict())

        inventory_plugin._update_cache("path1", [])
        inventory_plugin._update_cache("path2", [dict(a="a")])

        assert inventory_plugin._cache == dict(path1=[], path2=[dict(a="a")])


class TestInventoryModuleGetRecords:
    def test_fetch_records_from_sn(self, inventory_plugin, table_client):
        inventory_plugin._sn_table_client = table_client
        inventory_plugin._sn_table = "cmdb_ci"
        inventory_plugin._sn_query = "name=my_ci"
        inventory_plugin._is_encoded_query = True

        inventory_plugin._sn_enhanced = False

        table_client.list_records.return_value = [
            dict(sys_id="123abc", ip_address="1.2.3.4", fqdn="01.cmdb_ci.com")
        ]

        records = inventory_plugin._fetch_records_from_sn()

        assert records == [
            dict(sys_id="123abc", ip_address="1.2.3.4", fqdn="01.cmdb_ci.com")
        ]

    def test_fetch_records_from_sn_enhanced(self, inventory_plugin, table_client):
        inventory_plugin._sn_table_client = table_client
        inventory_plugin._sn_table = "cmdb_ci"
        inventory_plugin._sn_query = "name=my_ci"
        inventory_plugin._is_encoded_query = True

        inventory_plugin._sn_enhanced = True

        table_client.list_records.side_effect = [
            [dict(sys_id="123abc", ip_address="1.2.3.4", fqdn="01.cmdb_ci.com")],
            [
                {
                    "child.sys_id": "123abc",
                    "parent.name": "parent",
                    "parent.sys_class_name": "parent_sys_class_name",
                    "type.name": "Parent description::Child description",
                },
                {
                    "parent.sys_id": "123abc",
                    "child.name": "child",
                    "child.sys_class_name": "child_sys_class_name",
                    "type.name": "Parent description::Child description",
                },
            ],
        ]

        records = inventory_plugin._fetch_records_from_sn()

        assert records == [
            dict(
                sys_id="123abc",
                ip_address="1.2.3.4",
                fqdn="01.cmdb_ci.com",
                relationship_groups=set(
                    ["parent_Parent_description", "child_Child_description"]
                ),
            )
        ]

    @pytest.mark.parametrize(
        "user_cache,use_cache,get_cached_records_return_value,expected_cache_needs_update,expected_get_cache_records_call_count,expected_fetch_records_from_sn_call_count",
        [
            (True, True, [], False, 1, 0),
            (True, False, [], True, 0, 1),
            (False, True, [], False, 0, 1),
            (False, False, [], False, 0, 1),
            (True, True, None, True, 1, 1),
        ],
    )
    def test_get_records_from_sn_or_cache_flow(
        self,
        inventory_plugin,
        mocker,
        user_cache,
        use_cache,
        get_cached_records_return_value,
        expected_cache_needs_update,
        expected_get_cache_records_call_count,
        expected_fetch_records_from_sn_call_count,
    ):
        inventory_plugin._sn_cache = user_cache

        inventory_plugin._get_cached_records = mocker.Mock()
        inventory_plugin._get_cached_records.return_value = (
            get_cached_records_return_value
        )

        inventory_plugin._fetch_records_from_sn = mocker.Mock()
        inventory_plugin._fetch_records_from_sn.return_value = []

        records, cache_needs_update = inventory_plugin._get_records_from_sn_or_cache(
            "path1", use_cache
        )

        assert records == []
        assert cache_needs_update is expected_cache_needs_update

        inventory_plugin._get_cached_records.call_count == expected_get_cache_records_call_count
        inventory_plugin._fetch_records_from_sn.call_count == expected_fetch_records_from_sn_call_count

    @pytest.mark.parametrize(
        "user_cache,use_cache,cached_records,expected_records",
        [
            (True, True, [dict(a="a")], [dict(a="a")]),
            (True, False, [dict(a="a")], [dict(b="b")]),
            (False, True, [dict(a="a")], [dict(b="b")]),
            (False, False, [dict(a="a")], [dict(b="b")]),
            (True, True, None, [dict(b="b")]),
        ],
    )
    def test_get_records_from_sn_or_cache_records(
        self,
        inventory_plugin,
        mocker,
        user_cache,
        use_cache,
        cached_records,
        expected_records,
    ):
        inventory_plugin._sn_cache = user_cache
        inventory_plugin._get_cached_records = mocker.Mock()
        inventory_plugin._get_cached_records.return_value = cached_records

        inventory_plugin._fetch_records_from_sn = mocker.Mock()
        inventory_plugin._fetch_records_from_sn.return_value = [dict(b="b")]

        records, cnu = inventory_plugin._get_records_from_sn_or_cache(
            "path1", use_cache
        )

        assert records == expected_records


class TestPopulateInventory:
    def test_populate_inventory(self, inventory_plugin, mocker):
        inventory_plugin._sn_columns = "col1,col2,col3"
        inventory_plugin._sn_name_source = "name_source"
        inventory_plugin._sn_compose = dict(ansible_host="ip_address")
        inventory_plugin._sn_groups = dict(group1="ip_address == 1.1.1.1")
        inventory_plugin._sn_keyed_groups = [dict(key="key")]
        inventory_plugin._sn_strict = False
        inventory_plugin._sn_enhanced = True

        inventory_plugin.fill_constructed = mocker.Mock()

        records = [dict(ip_address="1.1.1.1")]

        inventory_plugin._populate_inventory(records)

        inventory_plugin.fill_constructed.assert_called_once_with(
            records,
            "col1,col2,col3",
            "name_source",
            dict(ansible_host="ip_address"),
            dict(group1="ip_address == 1.1.1.1"),
            [dict(key="key")],
            False,
            True,
        )


class TestInventoryModuleParse:
    @pytest.mark.parametrize(
        "use_cache,cache_needs_update,expected_update_cache_call_count",
        [(True, False, 0), (False, True, 1), (True, True, 1)],
    )
    def test_parse(
        self,
        inventory_plugin,
        mocker,
        use_cache,
        cache_needs_update,
        expected_update_cache_call_count,
    ):
        inventory_plugin._read_config_data = mocker.Mock()
        inventory_plugin.get_cache_key = mocker.Mock()
        inventory_plugin.get_cache_key.return_value = "cache_key"
        inventory_plugin._get_and_validate_options = mocker.Mock()
        inventory_plugin._make_table_client = mocker.Mock()
        inventory_plugin._get_records_from_sn_or_cache = mocker.Mock()
        inventory_plugin._get_records_from_sn_or_cache.return_value = [
            dict(a="a")
        ], cache_needs_update
        inventory_plugin._update_cache = mocker.Mock()
        inventory_plugin._populate_inventory = mocker.Mock()

        inventory_plugin.parse(None, None, "path1", cache=use_cache)

        inventory_plugin._read_config_data.assert_called_once_with("path1")
        inventory_plugin.get_cache_key.assert_called_once_with("path1")
        inventory_plugin._get_and_validate_options.assert_called_once()
        inventory_plugin._make_table_client.assert_called_once()
        inventory_plugin._get_records_from_sn_or_cache.assert_called_once_with(
            "cache_key", use_cache
        )
        inventory_plugin._update_cache.call_count == expected_update_cache_call_count
        if expected_update_cache_call_count:
            inventory_plugin._update_cache.assert_called_once_with(
                "cache_key", [dict(a="a")]
            )
        else:
            inventory_plugin._update_cache.assert_not_called()
        inventory_plugin._populate_inventory.assert_called_once_with([dict(a="a")])


class TestInventoryModuleFillEnhancedAutoGroups:
    def test_construction(self, inventory_plugin):
        record = dict(
            sys_id="1",
            fqdn="a1",
            relationship_groups=set(
                (
                    "NY-01-01_Rack_contains",
                    "Storage Area Network 002_Sends_data_to",
                    "Blackberry_Depends_on",
                    "Retail Adding Points_Depends_on",
                )
            ),
        )

        host = inventory_plugin.add_host(record, "fqdn")
        inventory_plugin.fill_enhanced_auto_groups(record, host)

        assert set(inventory_plugin.inventory.groups) == set(
            (
                "all",
                "ungrouped",
                "NY_01_01_Rack_contains",
                "Storage_Area_Network_002_Sends_data_to",
                "Blackberry_Depends_on",
                "Retail_Adding_Points_Depends_on",
            )
        )

        assert set(inventory_plugin.inventory.hosts) == set(("a1",))

        a1 = inventory_plugin.inventory.get_host("a1")
        a1_groups = (group.name for group in a1.groups)
        assert set(a1_groups) == set(
            (
                "NY_01_01_Rack_contains",
                "Storage_Area_Network_002_Sends_data_to",
                "Blackberry_Depends_on",
                "Retail_Adding_Points_Depends_on",
            )
        )

        assert a1.vars == dict(inventory_file=None, inventory_dir=None)

    def test_construction_empty(self, inventory_plugin):
        record = dict(sys_id="1", fqdn="a1", relationship_groups=set())

        host = inventory_plugin.add_host(record, "fqdn")
        inventory_plugin.fill_enhanced_auto_groups(record, host)

        assert set(inventory_plugin.inventory.groups) == set(("all", "ungrouped"))

        assert set(inventory_plugin.inventory.hosts) == set(("a1",))

        a1 = inventory_plugin.inventory.get_host("a1")
        a1_groups = (group.name for group in a1.groups)
        assert set(a1_groups) == set()

        assert a1.vars == dict(inventory_file=None, inventory_dir=None)


class TestInventoryModuleFillConstructed:
    def test_construction_empty(self, inventory_plugin):
        records = []
        columns = []
        name_source = "fqdn"
        compose = {}
        groups = {}
        keyed_groups = []
        strict = False
        enhanced = False

        inventory_plugin.fill_constructed(
            records,
            columns,
            name_source,
            compose,
            groups,
            keyed_groups,
            strict,
            enhanced,
        )

        assert set(inventory_plugin.inventory.groups) == set(("all", "ungrouped"))
        assert set(inventory_plugin.inventory.hosts) == set()

    def test_construction_host(self, inventory_plugin):
        records = [
            dict(
                sys_id="1",
                fqdn="a1",
            ),
            dict(
                sys_id="2",
                fqdn="a2",
            ),
        ]

        columns = []
        name_source = "fqdn"
        compose = {}
        groups = {}
        keyed_groups = []
        strict = False
        enhanced = False

        inventory_plugin.fill_constructed(
            records,
            columns,
            name_source,
            compose,
            groups,
            keyed_groups,
            strict,
            enhanced,
        )

        assert set(inventory_plugin.inventory.groups) == set(("all", "ungrouped"))
        assert set(inventory_plugin.inventory.hosts) == set(("a1", "a2"))

        a1 = inventory_plugin.inventory.get_host("a1")
        a1_groups = (group.name for group in a1.groups)
        assert set(a1_groups) == set()

        assert a1.vars == dict(inventory_file=None, inventory_dir=None)

        a2 = inventory_plugin.inventory.get_host("a2")
        a2_groups = (group.name for group in a2.groups)
        assert set(a2_groups) == set()

        assert a2.vars == dict(inventory_file=None, inventory_dir=None)

    def test_construction_hostvars(self, inventory_plugin):
        records = [
            dict(sys_id="1", fqdn="a1", cost="82", cost_cc="EUR"),
            dict(sys_id="2", fqdn="a2", cost="94", cost_cc="USD"),
        ]

        columns = ["cost", "cost_cc"]
        name_source = "fqdn"
        compose = {}
        groups = {}
        keyed_groups = []
        strict = False
        enhanced = False

        inventory_plugin.fill_constructed(
            records,
            columns,
            name_source,
            compose,
            groups,
            keyed_groups,
            strict,
            enhanced,
        )

        assert set(inventory_plugin.inventory.groups) == set(("all", "ungrouped"))
        assert set(inventory_plugin.inventory.hosts) == set(("a1", "a2"))

        a1 = inventory_plugin.inventory.get_host("a1")
        a1_groups = (group.name for group in a1.groups)
        assert set(a1_groups) == set()

        assert a1.vars == dict(
            inventory_file=None,
            inventory_dir=None,
            cost="82",
            cost_cc="EUR",
        )

        a2 = inventory_plugin.inventory.get_host("a2")
        a2_groups = (group.name for group in a2.groups)
        assert set(a2_groups) == set()

        assert a2.vars == dict(
            inventory_file=None,
            inventory_dir=None,
            cost="94",
            cost_cc="USD",
        )

    def test_construction_composite_vars(self, inventory_plugin):
        records = [
            dict(
                sys_id="1",
                fqdn="a1",
                cost="82",
                cost_cc="EUR",
                sys_updated_on="2021-09-17 02:13:25",
            ),
            dict(
                sys_id="2",
                fqdn="a2",
                cost="94",
                cost_cc="USD",
                sys_updated_on="2021-08-30 01:47:03",
            ),
        ]

        columns = []
        name_source = "fqdn"
        compose = dict(
            cost_res='"%s %s" % (cost, cost_cc)',
            amortized_cost="cost | int // 2",
            sys_updated_on_date="sys_updated_on | slice(2) | first | join",
            sys_updated_on_time="sys_updated_on | slice(2) | list | last | join | trim",
            silently_failed="non_existing + 3",
        )
        groups = {}
        keyed_groups = []
        strict = False
        enhanced = False

        inventory_plugin.fill_constructed(
            records,
            columns,
            name_source,
            compose,
            groups,
            keyed_groups,
            strict,
            enhanced,
        )

        assert set(inventory_plugin.inventory.groups) == set(("all", "ungrouped"))
        assert set(inventory_plugin.inventory.hosts) == set(("a1", "a2"))

        a1 = inventory_plugin.inventory.get_host("a1")
        a1_groups = (group.name for group in a1.groups)
        assert set(a1_groups) == set()

        assert a1.vars == dict(
            inventory_file=None,
            inventory_dir=None,
            cost_res="82 EUR",
            amortized_cost="41",
            sys_updated_on_date="2021-09-17",
            sys_updated_on_time="02:13:25",
        )

        a2 = inventory_plugin.inventory.get_host("a2")
        a2_groups = (group.name for group in a2.groups)
        assert set(a2_groups) == set()

        assert a2.vars == dict(
            inventory_file=None,
            inventory_dir=None,
            cost_res="94 USD",
            amortized_cost="47",
            sys_updated_on_date="2021-08-30",
            sys_updated_on_time="01:47:03",
        )

    def test_construction_composite_vars_strict(self, inventory_plugin):
        records = [
            dict(sys_id="1", fqdn="a1"),
            dict(sys_id="2", fqdn="a2"),
        ]

        columns = []
        name_source = "fqdn"
        compose = dict(failed="non_existing + 3")
        groups = {}
        keyed_groups = []
        strict = True
        enhanced = False

        with pytest.raises(AnsibleError, match="non_existing"):
            inventory_plugin.fill_constructed(
                records,
                columns,
                name_source,
                compose,
                groups,
                keyed_groups,
                strict,
                enhanced,
            )

    def test_construction_composite_vars_ansible_host(self, inventory_plugin):
        records = [
            dict(
                sys_id="1",
                fqdn="a1",
            ),
            dict(
                sys_id="2",
                fqdn="a2",
            ),
        ]

        columns = []
        name_source = "fqdn"
        compose = dict(ansible_host='fqdn + "_" + sys_id')
        groups = {}
        keyed_groups = []
        strict = False
        enhanced = False

        inventory_plugin.fill_constructed(
            records,
            columns,
            name_source,
            compose,
            groups,
            keyed_groups,
            strict,
            enhanced,
        )

        assert set(inventory_plugin.inventory.groups) == set(("all", "ungrouped"))
        assert set(inventory_plugin.inventory.hosts) == set(("a1", "a2"))

        a1 = inventory_plugin.inventory.get_host("a1")
        a1_groups = (group.name for group in a1.groups)
        assert set(a1_groups) == set()

        assert a1.vars == dict(
            inventory_file=None,
            inventory_dir=None,
            ansible_host="a1_1",
        )

        a2 = inventory_plugin.inventory.get_host("a2")
        a2_groups = (group.name for group in a2.groups)
        assert set(a2_groups) == set()

        assert a2.vars == dict(
            inventory_file=None,
            inventory_dir=None,
            ansible_host="a2_2",
        )

    def test_construction_composed_groups(self, inventory_plugin):
        records = [
            dict(sys_id="1", ip_address="1.1.1.1", fqdn="a1"),
            dict(sys_id="2", ip_address="1.1.1.2", fqdn="a2"),
        ]

        columns = []
        name_source = "fqdn"
        compose = {}
        groups = dict(
            ip1='ip_address == "1.1.1.1"',
            ip2='ip_address != "1.1.1.1"',
            cost="cost_usd < 90",  # ignored due to strict = False
        )
        keyed_groups = []
        strict = False
        enhanced = False

        inventory_plugin.fill_constructed(
            records,
            columns,
            name_source,
            compose,
            groups,
            keyed_groups,
            strict,
            enhanced,
        )

        assert set(inventory_plugin.inventory.groups) == set(
            ("all", "ungrouped", "ip1", "ip2")
        )

        assert set(inventory_plugin.inventory.hosts) == set(("a1", "a2"))

        a1 = inventory_plugin.inventory.get_host("a1")
        a1_groups = (group.name for group in a1.groups)
        assert set(a1_groups) == set(("ip1",))

        assert a1.vars == dict(inventory_file=None, inventory_dir=None)

        a2 = inventory_plugin.inventory.get_host("a2")
        a2_groups = (group.name for group in a2.groups)
        assert set(a2_groups) == set(("ip2",))

        assert a2.vars == dict(inventory_file=None, inventory_dir=None)

    def test_construction_composed_groups_strict(self, inventory_plugin):
        records = [
            dict(sys_id="1", ip_address="1.1.1.1", fqdn="a1"),
            dict(sys_id="2", ip_address="1.1.1.2", fqdn="a2"),
        ]

        columns = []
        name_source = "fqdn"
        compose = {}
        groups = dict(
            ip1='ip_address == "1.1.1.1"',
            ip2='ip_address != "1.1.1.1"',
            cost="cost_usd < 90",
        )
        keyed_groups = []
        strict = True
        enhanced = False

        with pytest.raises(AnsibleError, match="cost_usd"):
            inventory_plugin.fill_constructed(
                records,
                columns,
                name_source,
                compose,
                groups,
                keyed_groups,
                strict,
                enhanced,
            )

    def test_construction_keyed_groups(self, inventory_plugin):
        records = [
            dict(sys_id="1", fqdn="a1", cost_cc="EUR"),
            dict(sys_id="2", fqdn="a2", cost_cc="USD"),
        ]

        columns = []
        name_source = "fqdn"
        compose = {}
        groups = {}
        keyed_groups = [
            dict(
                key="cost_cc",
                default_value="EUR",
                prefix="cc",
            )
        ]
        strict = False
        enhanced = False

        inventory_plugin.fill_constructed(
            records,
            columns,
            name_source,
            compose,
            groups,
            keyed_groups,
            strict,
            enhanced,
        )

        assert set(inventory_plugin.inventory.groups) == set(
            ("all", "ungrouped", "cc_EUR", "cc_USD")
        )

        assert set(inventory_plugin.inventory.hosts) == set(("a1", "a2"))

        a1 = inventory_plugin.inventory.get_host("a1")
        a1_groups = (group.name for group in a1.groups)
        assert set(a1_groups) == set(("cc_EUR",))

        assert a1.vars == dict(inventory_file=None, inventory_dir=None)

        a2 = inventory_plugin.inventory.get_host("a2")
        a2_groups = (group.name for group in a2.groups)
        assert set(a2_groups) == set(("cc_USD",))

        assert a2.vars == dict(inventory_file=None, inventory_dir=None)

    def test_construction_keyed_groups_with_parent(self, inventory_plugin):
        records = [
            dict(sys_id="1", ip_address="1.1.1.1", fqdn="a1", cost_cc="EUR"),
            dict(sys_id="2", ip_address="1.1.1.2", fqdn="a2", cost_cc="USD"),
        ]

        columns = []
        name_source = "fqdn"
        compose = {}
        groups = {}
        keyed_groups = [
            dict(
                key="cost_cc",
                default_value="EUR",
                prefix="cc",
                parent_group="ip_address",
            )
        ]
        strict = False
        enhanced = False

        inventory_plugin.fill_constructed(
            records,
            columns,
            name_source,
            compose,
            groups,
            keyed_groups,
            strict,
            enhanced,
        )

        assert set(inventory_plugin.inventory.groups) == set(
            ("all", "ungrouped", "cc_EUR", "cc_USD", "ip_address")
        )

        assert set(inventory_plugin.inventory.hosts) == set(("a1", "a2"))

        a1 = inventory_plugin.inventory.get_host("a1")
        a1_groups = (group.name for group in a1.groups)
        assert set(a1_groups) == set(("cc_EUR", "ip_address"))

        assert a1.vars == dict(inventory_file=None, inventory_dir=None)

        a2 = inventory_plugin.inventory.get_host("a2")
        a2_groups = (group.name for group in a2.groups)
        assert set(a2_groups) == set(("cc_USD", "ip_address"))

        assert a2.vars == dict(inventory_file=None, inventory_dir=None)

    def test_construction_enhanced(self, inventory_plugin):
        records = [
            dict(
                sys_id="1",
                ip_address="1.1.1.1",
                fqdn="a1",
                relationship_groups=set(("NY-01-01_Rack_contains",)),
            ),
            dict(
                sys_id="2",
                ip_address="1.1.1.2",
                fqdn="a2",
                relationship_groups=set(
                    ("Storage Area Network 002_Sends_data_to", "OWA-SD-01_Runs_on")
                ),
            ),
        ]

        columns = []
        name_source = "fqdn"
        compose = {}
        groups = {}
        keyed_groups = []
        strict = False
        enhanced = True

        inventory_plugin.fill_constructed(
            records,
            columns,
            name_source,
            compose,
            groups,
            keyed_groups,
            strict,
            enhanced,
        )

        assert set(inventory_plugin.inventory.groups) == set(
            (
                "all",
                "ungrouped",
                "NY_01_01_Rack_contains",
                "Storage_Area_Network_002_Sends_data_to",
                "OWA_SD_01_Runs_on",
            )
        )

        assert set(inventory_plugin.inventory.hosts) == set(("a1", "a2"))

        a1 = inventory_plugin.inventory.get_host("a1")
        a1_groups = (group.name for group in a1.groups)
        assert set(a1_groups) == set(("NY_01_01_Rack_contains",))

        assert a1.vars == dict(inventory_file=None, inventory_dir=None)

        a2 = inventory_plugin.inventory.get_host("a2")
        a2_groups = (group.name for group in a2.groups)
        assert set(a2_groups) == set(
            ("Storage_Area_Network_002_Sends_data_to", "OWA_SD_01_Runs_on")
        )

        assert a2.vars == dict(inventory_file=None, inventory_dir=None)
