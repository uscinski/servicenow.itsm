# -*- coding: utf-8 -*-
# Copyright: (c) 2021, XLAB Steampunk <steampunk@xlab.si>
#
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function

from jinja2 import UndefinedError

__metaclass__ = type

import sys

import pytest

from ansible_collections.servicenow.itsm.plugins.module_utils import errors, table
from ansible_collections.servicenow.itsm.plugins.module_utils.client import Response

pytestmark = pytest.mark.skipif(
    sys.version_info < (2, 7), reason="requires python2.7 or higher"
)


class TestTableListRecords:
    def test_empty_response(self, client):
        client.get.return_value = Response(
            200, '{"result": []}', {"X-Total-Count": "0"}
        )
        t = table.TableClient(client)

        records = t.list_records("my_table")

        assert [] == records
        client.get.assert_called_once_with(
            "api/now/table/my_table",
            query=dict(
                sysparm_exclude_reference_link="true",
                sysparm_limit=1000,
                sysparm_offset=0,
            ),
        )

    def test_non_empty_response(self, client):
        client.get.return_value = Response(
            200, '{"result": [{"a": 3, "b": "sys_id"}]}', {"X-Total-Count": "1"}
        )
        t = table.TableClient(client)

        records = t.list_records("my_table")

        assert [dict(a=3, b="sys_id")] == records

    def test_query_passing(self, client):
        client.get.return_value = Response(
            200, '{"result": []}', {"X-Total-Count": "0"}
        )
        t = table.TableClient(client)

        t.list_records("my_table", dict(a="b"))

        client.get.assert_called_once_with(
            "api/now/table/my_table",
            query=dict(
                sysparm_exclude_reference_link="true",
                a="b",
                sysparm_limit=1000,
                sysparm_offset=0,
            ),
        )

    def test_pagination(self, client):
        client.get.side_effect = (
            Response(
                200, '{"result": [{"a": 3, "b": "sys_id"}]}', {"X-Total-Count": "2"}
            ),
            Response(
                200, '{"result": [{"a": 2, "b": "sys_ie"}]}', {"X-Total-Count": "2"}
            ),
        )
        t = table.TableClient(client, batch_size=1)

        records = t.list_records("my_table")

        assert [dict(a=3, b="sys_id"), dict(a=2, b="sys_ie")] == records
        assert 2 == len(client.get.mock_calls)
        client.get.assert_any_call(
            "api/now/table/my_table",
            query=dict(
                sysparm_exclude_reference_link="true", sysparm_limit=1, sysparm_offset=0
            ),
        )
        client.get.assert_any_call(
            "api/now/table/my_table",
            query=dict(
                sysparm_exclude_reference_link="true", sysparm_limit=1, sysparm_offset=1
            ),
        )


class TestTableGetRecord:
    def test_single_match(self, client):
        client.get.return_value = Response(
            200, '{"result": [{"a": 3, "b": "sys_id"}]}', {"X-Total-Count": "1"}
        )
        t = table.TableClient(client)

        record = t.get_record("my_table", dict(our="query"))

        assert dict(a=3, b="sys_id") == record
        client.get.assert_called_with(
            "api/now/table/my_table",
            query=dict(
                sysparm_exclude_reference_link="true",
                our="query",
                sysparm_limit=1000,
                sysparm_offset=0,
            ),
        )

    def test_multiple_matches(self, client):
        client.get.return_value = Response(
            200, '{"result": [{"a": 3}, {"b": 4}]}', {"X-Total-Count": "1"}
        )
        t = table.TableClient(client)

        with pytest.raises(errors.ServiceNowError, match="2"):
            t.get_record("my_table", dict(our="query"))

    def test_zero_matches(self, client):
        client.get.return_value = Response(
            200, '{"result": []}', {"X-Total-Count": "0"}
        )
        t = table.TableClient(client)

        assert t.get_record("my_table", dict(our="query")) is None

    def test_zero_matches_fail(self, client):
        client.get.return_value = Response(
            200, '{"result": []}', {"X-Total-Count": "0"}
        )
        t = table.TableClient(client)

        with pytest.raises(errors.ServiceNowError, match="No"):
            t.get_record("my_table", dict(our="query"), must_exist=True)


class TestTableCreateRecord:
    def test_normal_mode(self, client):
        client.post.return_value = Response(201, '{"result": {"a": 3, "b": "sys_id"}}')
        t = table.TableClient(client)

        record = t.create_record("my_table", dict(a=4), False)

        assert dict(a=3, b="sys_id") == record
        client.post.assert_called_with(
            "api/now/table/my_table",
            dict(a=4),
            query=dict(sysparm_exclude_reference_link="true"),
        )

    def test_check_mode(self, client):
        client.post.return_value = Response(201, '{"result": {"a": 3, "b": "sys_id"}}')
        t = table.TableClient(client)

        record = t.create_record("my_table", dict(a=4), True)

        assert dict(a=4) == record
        client.post.assert_not_called()


class TestTableUpdateRecord:
    def test_normal_mode(self, client):
        client.patch.return_value = Response(200, '{"result": {"a": 3, "b": "sys_id"}}')
        t = table.TableClient(client)

        record = t.update_record("my_table", dict(sys_id="id"), dict(a=4), False)

        assert dict(a=3, b="sys_id") == record
        client.patch.assert_called_with(
            "api/now/table/my_table/id",
            dict(a=4),
            query=dict(sysparm_exclude_reference_link="true"),
        )

    def test_check_mode(self, client):
        client.patch.return_value = Response(200, '{"result": {"a": 3, "b": "sys_id"}}')
        t = table.TableClient(client)

        record = t.update_record("my_table", dict(sys_id="id"), dict(a=4), True)

        assert dict(sys_id="id", a=4) == record
        client.patch.assert_not_called()


class TestTableDeleteRecord:
    def test_normal_mode(self, client):
        client.delete.return_value = Response(204, "")
        t = table.TableClient(client)

        t.delete_record("my_table", dict(sys_id="id"), False)

        client.delete.assert_called_with("api/now/table/my_table/id")

    def test_check_mode(self, client):
        client.delete.return_value = Response(204, "")
        t = table.TableClient(client)

        t.delete_record("my_table", dict(sys_id="id"), True)

        client.delete.assert_not_called()


class TestFindUser:
    def test_user_name_lookup(self, table_client):
        table_client.get_record.return_value = dict(sys_id="1234", user_name="test")

        user = table.find_user(table_client, "test")

        assert dict(sys_id="1234", user_name="test") == user



undefined_list = []
env = None
class TestTableListDottedRecords:
    @pytest.mark.parametrize(
        "record,keys,value,expected",
        [
            (dict(), ["a"], "a_val", {"a": {".": "a_val"}}),
            (dict(), ["a", "b"], "a.b_val", {"a": {"b": {".": "a.b_val"}}}),
            (dict(), ["a", "b", "c"], "a.b.c_val", {"a": {"b": {"c": {".": "a.b.c_val"}}}}),
            (dict(b="b_val"), ["a"], "a_val", {"a": {".": "a_val"}, "b": "b_val"}),
            (dict(a=dict(b="a.b_val")), ["a", "c"], "a.c_val", {"a": {"b": "a.b_val", "c": {".": "a.c_val"}}}),
            (
                dict(a=dict(b=dict(c="a.b.c_val", d="a.b.d_val"), e="a.e_val")),
                ["a", "b", "f"], "a.b.f_val",
                {"a": {"b": {"c": "a.b.c_val", "d": "a.b.d_val", "f": {".": "a.b.f_val"}}, "e": "a.e_val"}},
            ),
        ]
    )
    def test_record_rec(self, record, keys, value, expected):
        t = table.TableClient(None)
        t._record_rec(record, keys, value)
        assert record == expected

    @pytest.mark.parametrize(
        "record,expected",
        [
            (dict(), dict()),
            (dict(a="a_val"), {"a": {".": "a_val"}}),
            (
                {"a": "a_val", "b.c": "b.c_val"},
                {"a": {".": "a_val"}, "b.c": {".": "b.c_val"}, "b": {"c": {".": "b.c_val"}}}
            ),
            (
                {"a": "a_val", "a.b": "a.b_val"},
                {"a.b": {".": "a.b_val"}, "a": {".": "a_val", "b": {".": "a.b_val"}}},
            )
        ]
    )
    def test_record_dot2dict(self, record, expected):
        t = table.TableClient(None)
        actual = t._record_dot2dict(record)
        assert actual == expected

    def test_mydict(self):
        d = MyDict(a={".": "a_val"})
        assert d["a"] == "a_val"

    def test_jinja(self):
        from jinja2 import Environment, meta
        env = Environment()
        ast = env.parse("{{ a.b.c }}")

        n1 = ast.find(meta.nodes.Getattr)
        assert n1.attr == "c"
        assert n1.node.attr == "b"
        assert n1.node.node.name == "a"

        ast = env.parse("{{ 3 + e['f'] | string }}")

        n1 = ast.find(meta.nodes.Getitem)
        assert n1.arg.value == "f"
        assert n1.node.name == "e"

    def test_jinja_template(self):
        from jinja2 import Template
        t = Template("{{ a.b + 3 }}", finalize=self.finalize)

        res = t.render(dict(a=dict(b=6)))

        assert res == '9'

    def test_jinja_template_type_error(self):
        from jinja2 import Template
        t = Template("{{ a.b + 3 }}", finalize=self.finalize)

        with pytest.raises(TypeError):
            t.render(dict(a=dict(b={".value": 6})))

    def test_jinja_template_catch_undefined(self):
        from jinja2 import Template, ChainableUndefined
        import re
        t = Template("{{ a.b + 3 }}")

        try:
            t.render()
        except UndefinedError as undef_err:
            assert undef_err.message == "'a' is undefined"
            node_re = re.compile(r"'(\w+)' is undefined")
            m = node_re.match(undef_err.message)
            assert m.group(1) == "a"

        try:
            t.render(a='a_val')
        except UndefinedError as undef_err:
            assert undef_err.message == "'str object' has no attribute 'b'"
            attr_re = re.compile(r"'str object' has no attribute '(\w+)'")
            m = attr_re.match(undef_err.message)
            assert m.group(1) == "b"

        res = t.render(a=dict(b=2))
        assert res == '5'

    def test_jinja_template_catch_chainable_undefined(self):
        from jinja2 import Template, ChainableUndefined
        import re
        t = Template("{{ a.b + 3 }}", undefined=ChainableUndefined)

        try:
            t.render()
        except UndefinedError as undef_err:
            assert undef_err.message == "'a' is undefined"
            node_re = re.compile(r"'(\w+)' is undefined")
            m = node_re.match(undef_err.message)
            assert m.group(1) == "a"

        try:
            t.render(a='a_val')
        except UndefinedError as undef_err:
            assert undef_err.message == "'str object' has no attribute 'b'"
            attr_re = re.compile(r"'str object' has no attribute '(\w+)'")
            m = attr_re.match(undef_err.message)
            assert m.group(1) == "b"

        res = t.render(a=dict(b=2))
        assert res == '5'

    def finalize(self, var):
        return var


from collections import UserDict
class MyDict(UserDict):
    calls = []
    
    def __init__(self, *args, undefined_vars=[], **kwargs):
        self._previous = None
        self._undefined_vars = undefined_vars
        self._pos = 0
        super().__init__(*args, **kwargs)

    def __getitem__(self, key):
        d = self.data[key]
        if "." in d:
            return d["."]
        else:
            return d
