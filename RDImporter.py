from itertools import islice
import glob
import gevent
import netaddr
import shutil
import os
import time
import multiprocessing as mp

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


# String -> Dict
def get_ripe_inetnum_object(record):
    temp_object = get_empty_ripe_inetnum_object()

    for line in record.splitlines():
        for target_attribute in target_ripe_inetnum_attributes:
            target = target_attribute + ":"

            if line.startswith(target):
                attribute_value = line[len(target):].strip()
                temp_object[target_attribute] = attribute_value

    return temp_object


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
                inetnum_value = inetnum_value + "," + convert_to_cidr_block(inetnum_value)
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
                org_info = get_ripe_organisation_info(inetnum_value)

                if org_info is not None:
                    for org_key, org_value in org_info.iteritems():
                        if org_key is not "organisation":
                            org_values = org_values + '"' + str(org_value) + '"' + ","
                else:
                    print inetnum_object
            elif inetnum_key is not "country":
                inetnum_value = '"' + inetnum_value + '"'

        temp_record = temp_record + inetnum_value + ","

    temp_record = temp_record + org_values + route_values
    return temp_record[:-1] + "\n"


# String -> String
def convert_to_cidr_block(ip_range):
    ips = ip_range.split("-")
    start_ip = ips[0].strip()
    end_ip = ips[1].strip()
    return str(netaddr.iprange_to_cidrs(start_ip, end_ip)[0])


# String -> Dict/None
def get_ripe_organisation_info(org):
    object_count = -1
    temp_object = get_empty_ripe_organisation_object()

    with open(registry_data_directory + input_file_base_name + ".organisation", 'r') as f:
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

    with open(registry_data_directory + input_file_base_name + ".route", 'r') as f:
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


# [Int], Int -> [[Int], [Int], ...]
def split_list_into_n_parts(a, n):
    k, m = divmod(len(a), n)
    return list(a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in xrange(n))


# NON-CONCURRENT IMPORT
# None -> None
# writes (country, org, IP range, IP prefix, descr, netname, org_type, org_name, asn, as_descr) to CSV file
# filename: "output/ripe_registry_linear.txt"
def import_ripe_registry_data_linear():
    line_count = 0
    object_count = -1

    temp_object = get_empty_ripe_inetnum_object()

    with open(linear_output_file, "w") as dest_fp:
        with open(registry_data_directory + input_file_base_name + ".inetnum") as src_fp:
            for line in src_fp:
                if line_count > lines_to_process:
                    break

                for target_attribute in target_ripe_inetnum_attributes:
                    target = target_attribute + ":"

                    if line.startswith(target):
                        if target_attribute is target_ripe_inetnum_attributes[0]:
                            object_count = object_count + 1

                            if object_count >= 1:
                                record = evaluate_ripe_inetnum_object(temp_object)
                                dest_fp.write(record)
                                temp_object = get_empty_ripe_inetnum_object()

                        attribute_value = line[len(target):].strip()
                        temp_object[target_attribute] = attribute_value

                line_count = line_count + 1


# CONCURRENT IMPORT
# Int -> [[Int], [Int], ...]
def get_inetnum_record_boundaries(num_threads):
    line_count = 0
    boundaries = []

    with open(registry_data_directory + input_file_base_name + ".inetnum") as src_fp:
        for line in src_fp:
            if line_count > lines_to_process:
                break

            if line.startswith(target_ripe_inetnum_attributes[0] + ":"):
                boundaries.append(line_count)

            line_count = line_count + 1

    return split_list_into_n_parts(boundaries, num_threads)


# [Int], Int -> None
# writes (country, org, IP range, IP prefix, descr, netname, org_type, org_name, asn, as_descr) to CSV file
# filename: "output/concurrent_tmp/ripe_registry_part_X.txt"
def import_ripe_registry_data_in_range(record_boundaries, thread_num):
    line_count = 0
    record_count = 0
    filename = str(tmp_directory) + str(output_file_base_name) + "_part_" + str(thread_num) + ".txt"

    with open(filename, "w") as dest_fp:
        with open(registry_data_directory + input_file_base_name + ".inetnum") as src_fp:
            for line in src_fp:
                if (record_count+1) > len(record_boundaries):
                    break

                if line_count == record_boundaries[record_count]:
                    record = line + ''.join(islice(src_fp, 10))
                    dest_fp.write(evaluate_ripe_inetnum_object(get_ripe_inetnum_object(record)))
                    record_count = record_count + 1
                    line_count = line_count + 11
                else:
                    line_count = line_count + 1


# None -> None
# writes (country, org, IP range, IP prefix, descr, netname, org_type, org_name, asn, as_descr) to CSV file
# filename: "output/ripe_registry_concurrent.txt"
def import_ripe_registry_data_concurrent(num_threads):
    if os.path.exists(tmp_directory):
        shutil.rmtree(tmp_directory)

    os.makedirs(tmp_directory)

    record_boundaries = get_inetnum_record_boundaries(num_threads)

    threads = [gevent.spawn(import_ripe_registry_data_in_range(record_boundaries[i], i))
               for i in xrange(len(record_boundaries))]

    gevent.joinall(threads)

    with open(concurrent_output_file, 'wb') as dest_fp:
        for tmp_fp in glob.glob(tmp_files):
            if file == concurrent_output_file:
                continue
            with open(tmp_fp, 'rb') as src_fp:
                shutil.copyfileobj(src_fp, dest_fp)


output_file_base_name = "ripe_registry"
input_file_base_name = "ripe.db"
tmp_directory = "output/concurrent_tmp/"
tmp_files = "output/concurrent_tmp/*.txt"
concurrent_output_file = "output/ripe_registry_concurrent.txt"
linear_output_file = "output/ripe_registry_linear.txt"
linear_output = "output/ripe_registry_linear.txt"
lines_to_process = 100000
registry_data_directory = "RIPE Data/"


# MAIN
def main():
    # start_time = time.time()
    # import_ripe_registry_data_linear()
    # import_ripe_registry_data_concurrent(4)
    # print("--- %s seconds ---" % (time.time() - start_time))

    start_time = time.time()
    import_ripe_registry_data_concurrent_new()
    print("--- %s seconds ---" % (time.time() - start_time))


def process_wrapper(lineID):
    with open(registry_data_directory + input_file_base_name + ".inetnum") as src_fp:
        for i, line in enumerate(src_fp):
            if i != lineID:
                continue
            else:
                print line
                break


def experimental1():
    pool = mp.Pool(4)
    jobs = []
    line_count = 0

    with open(registry_data_directory + input_file_base_name + ".inetnum") as src_fp:
        for ID, line in enumerate(src_fp):
            if line_count > 200:
                for job in jobs:
                    job.get()

                pool.close()

            jobs.append(pool.apply_async(process_wrapper(ID)))
            line_count = line_count + 1

    return


def process_record(byte_position, write_queue):
    with open(registry_data_directory + input_file_base_name + ".inetnum") as src_fp:
        src_fp.seek(byte_position)
        record = src_fp.readline() + ''.join(islice(src_fp, 10))
        write_queue.put(evaluate_ripe_inetnum_object(get_ripe_inetnum_object(record)))


def listen_for_write_request(queue):
    with open(concurrent_output_file, "w") as dest_fp:
        while True:
            message = queue.get()
            if message is "EOF":
                break
            dest_fp.write(str(message))
            dest_fp.flush()
        dest_fp.close()


def import_ripe_registry_data_concurrent_new():
    manager = mp.Manager()
    write_queue = manager.Queue()

    pool = mp.Pool(mp.cpu_count())
    pool.apply_async(listen_for_write_request, [write_queue])

    jobs = []
    line_count = 0

    with open(registry_data_directory + input_file_base_name + ".inetnum") as src_fp:
        next_line_byte_position = 0

        for line in src_fp:
            if line.startswith(target_ripe_inetnum_attributes[0] + ":"):
                jobs.append(pool.apply_async(process_record, [next_line_byte_position, write_queue]))

            next_line_byte_position = next_line_byte_position + len(line)
            line_count = line_count + 1

    for job in jobs:
        job.get()

    write_queue.put("EOF")
    pool.close()


if __name__ == '__main__':
    main()
