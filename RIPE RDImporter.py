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
def get_empty_inetnum_object():
    return {
        "inetnum": None,
        "netname": None,
        "descr": None,
        "country": None,
        "org": None,
    }


# String -> Dict
def get_inetnum_object(record):
    temp_object = get_empty_inetnum_object()

    for line in record.splitlines():
        for target_attribute in target_ripe_inetnum_attributes:
            target = target_attribute + ":"

            if line.startswith(target):
                attribute_value = line[len(target):].strip()
                temp_object[target_attribute] = attribute_value

    return temp_object


# None -> Dict
def get_empty_organisation_object():
    return {
        "organisation": None,
        "org-name": None,
        "org-type": None,
    }


# None
def get_empty_route_object():
    return {
        "route": None,
        "descr": None,
        "origin": None
    }


# Dict -> String
def evaluate_inetnum_object(inetnum_object):
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
                org_info = get_organisation_info(inetnum_value)

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
def get_organisation_info(org):
    object_count = -1
    temp_object = get_empty_organisation_object()
    filename = registry_data_directory + file_base_name_registry_data + ".organisation"
    with open(filename, 'r') as f:
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
def get_route_info(ip):
    object_count = -1
    temp_object = get_empty_route_object()
    filename = registry_data_directory + file_base_name_registry_data + ".route"
    with open(filename, 'r') as f:
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
def import_registry_data_linear():
    line_count = 0
    object_count = -1

    temp_object = get_empty_inetnum_object()
    dest_filename = output_directory + file_base_name_output_linear + ".txt"
    src_filename = registry_data_directory + file_base_name_registry_data + ".inetnum"
    with open(dest_filename, "w") as dest_fp:
        with open(src_filename) as src_fp:
            for line in src_fp:
                if line_count > lines_to_process:
                    break

                for target_attribute in target_ripe_inetnum_attributes:
                    target = target_attribute + ":"

                    if line.startswith(target):
                        if target_attribute is target_ripe_inetnum_attributes[0]:
                            object_count = object_count + 1

                            if object_count >= 1:
                                record = evaluate_inetnum_object(temp_object)
                                dest_fp.write(record)
                                temp_object = get_empty_inetnum_object()

                        attribute_value = line[len(target):].strip()
                        temp_object[target_attribute] = attribute_value

                line_count = line_count + 1


# CONCURRENT IMPORT
# Int -> [[Int], [Int], ...]
def get_inetnum_record_boundaries(num_threads):
    line_count = 0
    boundaries = []
    filename = registry_data_directory + file_base_name_registry_data + ".inetnum"
    with open(filename) as src_fp:
        for line in src_fp:
            if line_count > lines_to_process:
                break

            if line.startswith(target_ripe_inetnum_attributes[0] + ":"):
                boundaries.append(line_count)

            line_count = line_count + 1

    return split_list_into_n_parts(boundaries, num_threads)


# [Int], Int -> None
def import_registry_data_in_range(record_boundaries, thread_num):
    line_count = 0
    record_count = 0

    dest_filename = str(tmp_directory) + str(file_base_name_output_tmp) + "_part_" + str(thread_num) + ".txt"
    src_filename = registry_data_directory + file_base_name_registry_data + ".inetnum"

    with open(dest_filename, "w") as dest_fp:
        with open(src_filename) as src_fp:
            for line in src_fp:
                if (record_count+1) > len(record_boundaries):
                    break

                if line_count == record_boundaries[record_count]:
                    record = line + ''.join(islice(src_fp, 10))
                    dest_fp.write(evaluate_inetnum_object(get_inetnum_object(record)))
                    record_count = record_count + 1
                    line_count = line_count + 11
                else:
                    line_count = line_count + 1


# None -> None
def import_registry_data_with_concurrent_thread(num_threads):
    if os.path.exists(tmp_directory):
        shutil.rmtree(tmp_directory)

    os.makedirs(tmp_directory)

    record_boundaries = get_inetnum_record_boundaries(num_threads)

    threads = [gevent.spawn(import_registry_data_in_range(record_boundaries[i], i))
               for i in xrange(len(record_boundaries))]

    gevent.joinall(threads)

    dest_filename = output_directory + file_base_name_output_concurrent + "_thread.txt"
    with open(dest_filename, 'wb') as dest_fp:
        for tmp_fp in glob.glob(tmp_directory + "*.txt"):
            with open(tmp_fp, 'rb') as src_fp:
                shutil.copyfileobj(src_fp, dest_fp)


# Byte, multiprocessing.Queue -> None
def process_record(byte_position, write_queue):
    src_filename = registry_data_directory + file_base_name_registry_data + ".inetnum"
    with open(src_filename) as src_fp:
        src_fp.seek(byte_position)
        record = src_fp.readline() + ''.join(islice(src_fp, 10))
        write_queue.put(evaluate_inetnum_object(get_inetnum_object(record)))


def process_record_fast(record, write_queue):
    processed_record = evaluate_inetnum_object(get_inetnum_object(record))
    write_queue.put(processed_record)
    return


# multiprocessing.Queue -> None
def listen_for_write_request(queue):
    dest_filename = output_directory + file_base_name_output_concurrent + "_process.txt"
    with open(dest_filename, "w") as dest_fp:
        while True:
            message = queue.get()
            if message is "EOF":
                break
            dest_fp.write(str(message))
            dest_fp.flush()
        dest_fp.close()


# None -> None
def import_registry_data_with_concurrent_process():
    manager = mp.Manager()
    write_queue = manager.Queue()

    pool = mp.Pool(mp.cpu_count())
    pool.apply_async(listen_for_write_request, [write_queue])

    jobs = []
    line_count = 0

    with open(registry_data_directory + file_base_name_registry_data + ".inetnum") as src_fp:
        next_line_byte_position = 0

        for line in src_fp:
            if line_count > lines_to_process:
                break

            if line.startswith(target_ripe_inetnum_attributes[0] + ":"):
                record = line + ''.join(islice(src_fp, 10))
                # jobs.append(pool.apply_async(process_record, [next_line_byte_position, write_queue]))
                jobs.append(pool.apply_async(process_record_fast, [record, write_queue]))

            next_line_byte_position = next_line_byte_position + len(line)
            line_count = line_count + 1

    for job in jobs:
        job.get()

    write_queue.put("EOF")
    pool.close()


file_base_name_registry_data = "ripe.db"
file_base_name_output_tmp = "ripe_registry"
file_base_name_output_linear = "ripe_registry_linear"
file_base_name_output_concurrent = "ripe_registry_concurrent"

registry_data_directory = "data/"
tmp_directory = "tmp/"
output_directory = "output/"

lines_to_process = 1000000


# MAIN
def main():
    # writes (country, org, IP range, IP prefix, descr, netname, org_type, org_name, asn, as_descr) to tmp CSV file

    # start_time = time.time()
    # import_registry_data_linear()
    # print("--- %s seconds ---" % (time.time() - start_time))
    #
    # start_time = time.time()
    # import_registry_data_with_concurrent_thread(8)
    # print("--- %s seconds ---" % (time.time() - start_time))

    start_time = time.time()
    # import_registry_data_with_concurrent_thread(8)
    # import_registry_data_linear()
    import_registry_data_with_concurrent_process()
    print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == '__main__':
    main()
