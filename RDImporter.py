import netaddr

inetnum_attributes = (
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

target_inetnum_attributes = (
    "inetnum",
    "netname",
    "descr",
    "country",
    "org"
)


# HELPERS
# None -> Dict
def get_empty_inetnum_object():
    temp_object = {
        "inetnum": None,
        "netname": None,
        "descr": None,
        "country": None,
        "org": None,
    }

    return temp_object


# Dict -> String
def evaluate_inetnum_object(inetnum_object):
    temp_record = ""

    for key, value in inetnum_object.iteritems():
        if value is None:
            value = "NULL"
        else:
            if key is "inetnum":
                value = convert_to_cidr_block(value)
            elif key not in { "country", "org"}:
                value = '"' + value + '"'

        temp_record = temp_record + value + ","

    return temp_record[:-1]


# String -> String
def convert_to_cidr_block(ip_range):
    ips = ip_range.split("-")
    start_ip = ips[0].strip()
    end_ip = ips[1].strip()
    return str(netaddr.iprange_to_cidrs(start_ip, end_ip)[0])


# IMPORT
# None -> None
# (country, org, inetnum, descr, netname)
def import_ripe_inetnum():
    line_count = 0
    object_count = -1

    records = ""
    temp_object = get_empty_inetnum_object()

    with open("output/ripe_registry.txt", "w") as dest_fp:
        with open('RIPE Data/ripe.db.inetnum') as src_fp:
            for line in src_fp:
                if line_count > 1000:
                    break

                for target_attribute in target_inetnum_attributes:
                    target = target_attribute + ":"

                    if line.startswith(target):
                        if target_attribute is target_inetnum_attributes[0]:
                            object_count = object_count + 1

                            if object_count >= 1:
                                record = evaluate_inetnum_object(temp_object)
                                records = records + record + "\n"
                                dest_fp.write(record + "\n")
                                temp_object = get_empty_inetnum_object()

                        attribute_value = line[len(target):].strip()
                        temp_object[target_attribute] = attribute_value

                line_count = line_count + 1

    print records, object_count


# MAIN
def main():
    import_ripe_inetnum()


if __name__ == '__main__':
    main()
