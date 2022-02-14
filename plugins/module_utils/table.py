# -*- coding: utf-8 -*-
# Copyright: (c) 2021, XLAB Steampunk <steampunk@xlab.si>
#
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function

__metaclass__ = type

from . import errors


def _path(table, *subpaths):
    return "/".join(("api", "now") + ("table", table) + subpaths)


def _query(original=None):
    # Flatten the response (skip embedded links to resources)
    return dict(original or {}, sysparm_exclude_reference_link="true")


class TableClient:
    def __init__(self, client, batch_size=1000):
        # 1000 records is default batch size for ServiceNow REST API, so we also use it
        # as a default.
        self.client = client
        self.batch_size = batch_size

    def list_records(self, table, query=None, dot2dict=False):
        base_query = _query(query)
        base_query["sysparm_limit"] = self.batch_size

        offset = 0
        total = 1  # Dummy value that ensures loop executes at least once
        result = []

        while offset < total:
            response = self.client.get(
                _path(table), query=dict(base_query, sysparm_offset=offset)
            )

            result.extend(response.json["result"])
            total = int(response.headers["x-total-count"])
            offset += self.batch_size

        # TODO: dotted keys to nested dictionaries
        if dot2dict:
            result = self._dot2dict(result)
        return result

    def _dot2dict(self, records):
        for i, record in enumerate(records):
            records[i] = self._record_dot2dict(record)

    def _record_dot2dict(self, record):
        out_record = dict()
        for k, v in record.items():
            out_record[k] = v  # copy original key-value pairs

            self._record_rec(out_record, k.split("."), v)

        return out_record

    def _record_rec(self, nested_record, dotted_keys, value):
        if len(dotted_keys) == 0:
            assert False  # should not happen
        elif len(dotted_keys) == 1:
            key = dotted_keys[0]
            nested_record[key] = value
        else:
            key = dotted_keys[0]
            r = nested_record.get(key, dict())
            self._record_rec(r, dotted_keys[1:], value)
            nested_record[key] = r

    def get_record(self, table, query, must_exist=False):
        records = self.list_records(table, query)

        if len(records) > 1:
            raise errors.ServiceNowError(
                "{0} {1} records match the {2} query.".format(
                    len(records), table, query
                )
            )

        if must_exist and not records:
            raise errors.ServiceNowError(
                "No {0} records match the {1} query.".format(table, query)
            )

        return records[0] if records else None

    def create_record(self, table, payload, check_mode):
        if check_mode:
            # Approximate the result using the payload.
            return payload

        return self.client.post(_path(table), payload, query=_query()).json["result"]

    def update_record(self, table, record, payload, check_mode):
        if check_mode:
            # Approximate the result by manually patching the existing state.
            return dict(record, **payload)

        return self.client.patch(
            _path(table, record["sys_id"]), payload, query=_query()
        ).json["result"]

    def delete_record(self, table, record, check_mode):
        if not check_mode:
            self.client.delete(_path(table, record["sys_id"]))


def find_user(table_client, user_id):
    # TODO: Maybe add a lookup-by-email option too?
    return table_client.get_record("sys_user", dict(user_name=user_id), must_exist=True)


def find_assignment_group(table_client, assignment_id):
    return table_client.get_record(
        "sys_user_group", dict(name=assignment_id), must_exist=True
    )


def find_standard_change_template(table_client, template_name):
    return table_client.get_record(
        "std_change_producer_version",
        dict(name=template_name),
        must_exist=True,
    )
