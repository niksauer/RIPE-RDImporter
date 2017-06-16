import netaddr
import ipcalc

ripe_inetnum_attributes = (
    "inetnum",
    "netname",
    "descr",
    "country",
    "geoloc",
    "language",
    "org",
    "sponsoring-org",
    "admin-c",
    "tech-c",
    "status",
    "remarks",
    "notify",
    "mnt-by",
    "mnt-lower",
    "mnt-routes",
    "mnt-domains",
    "mnt-irt",
    "created",
    "last-modified",
    "source"
)

target_ripe_inetnum_attributes = (
    "inetnum",
    "netname",
    "descr",
    "country",
    "org"
)

ripe_organisation_attributes = (
    "organisation",
    "org-name",
    "org-type",
    "descr",
    "remarks",
    "address",
    "phone",
    "fax-no",
    "e-mail",
    "geoloc",
    "language",
    "org",
    "admin-c",
    "tech-c",
    "abuse-c",
    "ref-nfy",
    "mnt-ref",
    "notify",
    "abuse-mailbox",
    "mnt-by",
    "created",
    "last-modified",
    "source"
)

target_ripe_organisation_attributes = (
    "organisation",
    "org-name",
    "org-type",
)

ripe_route_attributes = (
    "route",
    "descr",
    "origin",
    "pingable",
    "ping-hdl",
    "holes",
    "org",
    "member-of",
    "inject",
    "aggr-mtd",
    "aggr-bndry",
    "export-comps",
    "components",
    "remarks",
    "notify",
    "mnt-lower"
    "mnt-routes",
    "mnt-by",
    "changed",
    "created",
    "last-modified",
    "source"
)

target_ripe_route_attributes = (
    "route",
    "descr",
    "origin"
)


# HELPERS
# None -> Dict
def get_empty_ripe_inetnum_object():
    return {
        "inetnum": None,
        "netname": None,
        "descr": None,
        "country": None,
        "org": None,
    }


# None -> Dict
def get_empty_ripe_organisation_object():
    return {
        "organisation": None,
        "org-name": None,
        "org-type": None,
    }


# None
def get_empty_ripe_route_object():
    return {
        "route": None,
        "descr": None,
        "origin": None
    }


# Dict -> String
def evaluate_ripe_inetnum_object(inetnum_object):
    temp_record = ""
    org_values = ""
    route_values = ""

    for inetnum_key, inetnum_value in inetnum_object.iteritems():
        if inetnum_value is None:
            if inetnum_key is "org":
                org_values = "NULL,NULL,"
            inetnum_value = "NULL"
        else:
            if inetnum_key is "inetnum":
                inetnum_value = convert_to_cidr_block(inetnum_value)
                # TAKES LONG ??
                # route_info = get_ripe_route_info(str(ipcalc.IP(inetnum_value)))
                #
                # if route_info is not None:
                #     for route_key, route_value in route_info.iteritems():
                #         if route_key is not "route":
                #             route_values = route_values + '"' + str(route_value) + '"' + ","
                # else:
                #     route_values = "NULL,NULL,"
            elif inetnum_key is "org":
                for org_key, org_value in get_ripe_organisation_info(inetnum_value).iteritems():
                    if org_key is not "organisation":
                        org_values = org_values + '"' + str(org_value) + '"' + ","
            elif inetnum_key is not "country":
                inetnum_value = '"' + inetnum_value + '"'

        temp_record = temp_record + inetnum_value + ","

    temp_record = temp_record + org_values + route_values
    return temp_record[:-1]


# String -> String
def convert_to_cidr_block(ip_range):
    ips = ip_range.split("-")
    start_ip = ips[0].strip()
    end_ip = ips[1].strip()
    return str(netaddr.iprange_to_cidrs(start_ip, end_ip)[0])


# String -> Dict
def get_ripe_organisation_info(org):
    object_count = -1
    temp_object = get_empty_ripe_organisation_object()

    with open('RIPE Data/ripe.db.organisation', 'r') as f:
        for line in f:
            if line.__contains__(org):
                next_line = line.strip()

                for i in range(30):
                    for target_attribute in target_ripe_organisation_attributes:
                        target = target_attribute + ":"

                        if next_line.startswith(target):
                            if target_attribute is target_ripe_organisation_attributes[0]:
                                object_count = object_count + 1

                                if object_count == 1:
                                    return temp_object

                            attribute_value = next_line[len(target):].strip()
                            temp_object[target_attribute] = attribute_value

                    next_line = f.next().strip()


# String -> Dict
def get_ripe_route_info(ip):
    object_count = -1
    temp_object = get_empty_ripe_route_object()

    with open('RIPE Data/ripe.db.route', 'r') as f:
        for line in f:
            if line.__contains__(ip):
                next_line = line.strip()

                for i in range(30):
                    for target_attribute in target_ripe_route_attributes:
                        target = target_attribute + ":"

                        if next_line.startswith(target):
                            if target_attribute is target_ripe_route_attributes[0]:
                                object_count = object_count + 1

                                if object_count == 1:
                                    return temp_object

                            attribute_value = next_line[len(target):].strip()
                            temp_object[target_attribute] = attribute_value

                    next_line = f.next().strip()


# IMPORT
# None -> None
# writes (country, org, inetnum, descr, netname) to CSV file
def import_ripe_registry_data():
    line_count = 0
    object_count = -1

    records = ""
    temp_object = get_empty_ripe_inetnum_object()

    with open("output/ripe_registry.txt", "w") as dest_fp:
        with open('RIPE Data/ripe.db.inetnum') as src_fp:
            for line in src_fp:
                if line_count > 200:
                    break

                for target_attribute in target_ripe_inetnum_attributes:
                    target = target_attribute + ":"

                    if line.startswith(target):
                        if target_attribute is target_ripe_inetnum_attributes[0]:
                            object_count = object_count + 1

                            if object_count >= 1:
                                record = evaluate_ripe_inetnum_object(temp_object)
                                records = records + record + "\n"
                                dest_fp.write(record + "\n")
                                temp_object = get_empty_ripe_inetnum_object()

                        attribute_value = line[len(target):].strip()
                        temp_object[target_attribute] = attribute_value

                line_count = line_count + 1

    print records, object_count


# MAIN
def main():
    import_ripe_registry_data()

if __name__ == '__main__':
    main()
