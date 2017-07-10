from itertools import islice
import datetime

import configparser
import ipcalc
import netaddr
import os
import time
import multiprocessing

# source: https://www.iana.org/assignments/iana-ipv4-special-registry/iana-ipv4-special-registry.xhtml
special_purpose_networks = (
    '0.0.0.0/8',
    '10.0.0.0/8',
    '100.64.0.0/10',
    '127.0.0.0/8',
    '169.254.0.0/16',
    '172.16.0.0/12',
    '192.0.0.0/24',
    '192.0.2.0/24',
    '192.168.0.0/16',
    '192.175.48.0/24',
    '198.18.0.0/15',
    '198.51.100.0/24',
    '203.0.113.0/24',
    '224.0.0.0/4',
    '240.0.0.0/4',
    '255.255.255.255/32',
)

ip_exceptions = (
    '0.0.0.0',
)

# TODO: auto-retrieve list via VSQL query
non_ripe_networks = (
    '189.0.0.0/8', '182.0.0.0/8', '124.0.0.0/8', '120.0.0.0/8', '119.0.0.0/8', '118.0.0.0/8', '115.0.0.0/8',
    '114.0.0.0/8', '106.0.0.0/8', '102.0.0.0/8', '190.0.0.0/8', '184.0.0.0/8', '183.0.0.0/8', '181.0.0.0/8',
    '180.0.0.0/8', '179.0.0.0/8', '174.0.0.0/8', '133.0.0.0/8', '126.0.0.0/8', '123.0.0.0/8', '116.0.0.0/8',
    '112.0.0.0/8', '110.0.0.0/8', '191.0.0.0/8', '187.0.0.0/8', '186.0.0.0/8', '177.0.0.0/8', '175.0.0.0/8',
    '125.0.0.0/8', '122.0.0.0/8', '121.0.0.0/8', '117.0.0.0/8', '111.0.0.0/8', '108.0.0.0/8', '105.0.0.0/8',
    '101.0.0.0/8', '71.0.0.0/8', '70.0.0.0/8', '41.0.0.0/8', '35.0.0.0/8', '30.0.0.0/8', '29.0.0.0/8', '27.0.0.0/8',
    '20.0.0.0/8', '11.0.0.0/8', '6.0.0.0/8', '221.0.0.0/8', '215.0.0.0/8', '211.0.0.0/8', '98.0.0.0/8', '97.0.0.0/8',
    '76.0.0.0/8', '74.0.0.0/8', '73.0.0.0/8', '56.0.0.0/8', '47.0.0.0/8', '44.0.0.0/8', '39.0.0.0/8', '34.0.0.0/8',
    '33.0.0.0/8', '26.0.0.0/8', '22.0.0.0/8', '21.0.0.0/8', '19.0.0.0/8', '16.0.0.0/8', '15.0.0.0/8', '12.0.0.0/8',
    '8.0.0.0/8', '4.0.0.0/8', '3.0.0.0/8', '222.0.0.0/8', '220.0.0.0/8', '219.0.0.0/8', '214.0.0.0/8', '202.0.0.0/8',
    '200.0.0.0/8', '99.0.0.0/8', '75.0.0.0/8', '72.0.0.0/8', '68.0.0.0/8', '61.0.0.0/8', '60.0.0.0/8', '59.0.0.0/8',
    '58.0.0.0/8', '55.0.0.0/8', '54.0.0.0/8', '50.0.0.0/8', '49.0.0.0/8', '48.0.0.0/8', '42.0.0.0/8', '40.0.0.0/8',
    '38.0.0.0/8', '36.0.0.0/8', '32.0.0.0/8', '28.0.0.0/8', '18.0.0.0/8', '17.0.0.0/8', '14.0.0.0/8', '9.0.0.0/8',
    '7.0.0.0/8', '1.0.0.0/8', '223.0.0.0/8', '218.0.0.0/8', '210.0.0.0/8', '201.0.0.0/8', '197.0.0.0/8'
)


# -- NETWORK/IP HELPERS -- #
# Network, Network -> Bool
def is_subnet_of(a, b):
    comp_network = ipcalc.Network(str(a.network()) + '/' + str(b.mask))
    return a.mask >= b.mask and comp_network.network() == b.network()


# Network -> Bool
def is_special_purpose_network(network):
    for special_network in special_purpose_networks:
        if is_subnet_of(network, ipcalc.Network(special_network)):
            return True

    return False


# IP -> Bool
def is_known_ip_exception(ip):
    for exception in ip_exceptions:
        if ip == ipcalc.IP(exception):
            return True

    return False


# Network -> Bool
def is_foreign_network(network):
    for non_ripe_network in non_ripe_networks:
        if network in ipcalc.Network(non_ripe_network):
            return True

    return False


# -- RECORD HELPERS -- #
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


# None -> Dict
def get_empty_organisation_object():
    return {
        "organisation": None,
        "org-name": None,
        "org-type": None,
    }


# None -> Dict
def get_empty_route_object():
    return {
        "route": None,
        "descr": None,
        "origin": None
    }


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
# TODO: honor multi line attributes
def get_inetnum_object(record):
    temp_object = get_empty_inetnum_object()

    for line in record.splitlines():
        for target_attribute in target_ripe_inetnum_attributes:
            target = target_attribute + ":"

            if line.startswith(target):
                attribute_value = line[len(target):].strip()
                temp_object[target_attribute] = attribute_value

    return temp_object


# Dict -> String/None
def evaluate_inetnum_object(inetnum_object, failed_organisation_lookup_write_queue, exceptions_write_queue):
    temp_record = ""
    org_values = ""
    route_values = ""

    split_range = split_ip_range(inetnum_object['inetnum'])
    start_ip = split_range[0]
    end_ip = split_range[1]
    ip_prefix = convert_to_ip_prefix(inetnum_object['inetnum'])

    if is_special_purpose_network(ipcalc.Network(ip_prefix)) or is_known_ip_exception(ipcalc.IP(start_ip)):
        exceptions_write_queue.put(ip_prefix)
        return None
    else:
        for inetnum_key, inetnum_value in inetnum_object.iteritems():
            if inetnum_value is None:
                inetnum_value = "NULL"

                if inetnum_key is "org":
                    org_values = "NULL" + column_delimiter + "NULL" + column_delimiter
            else:
                if inetnum_key is "inetnum":
                    inetnum_value = start_ip + column_delimiter + end_ip + column_delimiter + ip_prefix
                elif inetnum_key is "org":
                    org_info = get_organisation_info(inetnum_value)

                    if org_info is not None:
                        for org_key, org_value in org_info.iteritems():
                            if org_key is not "organisation":
                                org_values = org_values + '"' + str(org_value) + '"' + column_delimiter
                    else:
                        org_values = "NULL" + column_delimiter + "NULL" + column_delimiter
                        failed_organisation_lookup_write_queue.put(inetnum_object)
                elif inetnum_key is not "country":
                    inetnum_value = '"' + inetnum_value + '"'

            temp_record = temp_record + inetnum_value + column_delimiter

        temp_record = temp_record + org_values + route_values
        return temp_record[:-1] + "\n"


# -- HELPERS -- #
# String -> String
def convert_to_ip_prefix(ip_range):
    ips = ip_range.split("-")
    start_ip = ips[0].strip()
    end_ip = ips[1].strip()
    return str(netaddr.iprange_to_cidrs(start_ip, end_ip)[0])


# String -> (String, String)
def split_ip_range(ip_range):
    ips = ip_range.split("-")
    start_ip = ips[0].strip()
    end_ip = ips[1].strip()
    return start_ip, end_ip


# [Int], Int -> [[Int], [Int], ...]
def split_list_into_n_parts(a, n):
    k, m = divmod(len(a), n)
    return list(a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in xrange(n))


# -- REFERENTIAL DATA HELPERS -- #
# String -> Dict/None
# TODO: use case-insensitive lookup for higher hit rate
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


# -- CONCURRENT MULTI-PROCESSED DATA IMPORT -- #
# Byte, multiprocessing.Queue -> None
def process_record_position(byte_position, write_queue, failed_organisation_lookup_write_queue, exceptions_write_queue):
    src_filename = registry_data_directory + file_base_name_registry_data + ".inetnum"

    with open(src_filename) as src_fp:
        src_fp.seek(byte_position)
        record = src_fp.readline() + ''.join(islice(src_fp, 10))
        processed_record = evaluate_inetnum_object(get_inetnum_object(record), failed_organisation_lookup_write_queue, exceptions_write_queue)

        if processed_record is not None:
            write_queue.put(processed_record)


# multiprocessing.Queue -> None
def listen_for_record_write_request(queue):
    dest_filename = output_directory + file_base_name_output + file_base_name_ending
    with open(dest_filename, "w") as dest_fp:
        while True:
            message = queue.get()
            if message is "EOF":
                break
            dest_fp.write(str(message))
            dest_fp.flush()
        dest_fp.close()


# multiprocessing.Queue -> None
def listen_for_failed_organisation_lookup_write_request(queue):
    dest_filename = output_directory + file_base_name_output_failed_lookup + file_base_name_ending
    with open(dest_filename, "w") as dest_fp:
        while True:
            message = queue.get()
            if message is "EOF":
                break
            dest_fp.write(str(message) + "\n")
            dest_fp.flush()
        dest_fp.close()


# multiprocessing.Queue -> None
def listen_for_exception_write_request(queue):
    dest_filename = output_directory + file_base_name_output_exception + file_base_name_ending

    with open(dest_filename, "w") as dest_fp:
        while True:
            message = queue.get()
            if message is "EOF":
                break
            dest_fp.write(str(message) + "\n")
            dest_fp.flush()
        dest_fp.close()


# None -> None
def import_ripe_registry_data():
    start_time = time.time()
    manager = multiprocessing.Manager()
    write_queue = manager.Queue()
    failed_organisation_lookup_write_queue = manager.Queue()
    exceptions_write_queue = manager.Queue()

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    pool.apply_async(listen_for_record_write_request, [write_queue])
    pool.apply_async(listen_for_failed_organisation_lookup_write_request, [failed_organisation_lookup_write_queue])
    pool.apply_async(listen_for_exception_write_request, [exceptions_write_queue])
    write_queue.put("# country, org, start IP, end IP, IP prefix, descr, netname, org_type, org_name\n")

    jobs = []
    line_count = 0

    with open(registry_data_directory + file_base_name_registry_data + ".inetnum") as src_fp:
        next_line_byte_position = 0

        for line in src_fp:
            if line_count > lines_to_process > 0:
                break

            if line.startswith(target_ripe_inetnum_attributes[0] + ":"):
                jobs.append(pool.apply_async(process_record_position,
                                             [
                                                 next_line_byte_position,
                                                 write_queue,
                                                 failed_organisation_lookup_write_queue,
                                                 exceptions_write_queue
                                             ]))

            next_line_byte_position = next_line_byte_position + len(line)
            line_count = line_count + 1

    for job in jobs:
        job.get()

    line_msg = str(lines_to_process)

    if lines_to_process == 0:
        line_msg = "all"

    execution_time = time.time() - start_time
    print("--- processed %s lines in %s seconds ---" % (line_msg, execution_time))

    write_queue.put("EOF --- processed %s lines in %s seconds ---" % (line_msg, execution_time))
    failed_organisation_lookup_write_queue.put("EOF")
    exceptions_write_queue.put("EOF")

    pool.close()


# -- CONCURRENT MULTI-PROCESSED POST-PROCESSING OF IMPORTED DATA -- #
def post_process_ripe_registry_data():
    # MULTIPROCESSING SETUP
    start_time = time.time()
    manager = multiprocessing.Manager()
    write_queue = manager.Queue()

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    # FILE SETUP
    src_filename = output_directory + file_base_name_output + "_" + file_date_for_post_processing + ".txt"

    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    pool.apply_async(listen_for_final_record_write_request, [write_queue])
    write_queue.put("# country_code, org_code, start_ip, end_ip, ip_prefix, descr, netname, org_type, org_name\n")

    # ALGORITHM SETUP
    line_count = 0

    with open(src_filename) as src_fp:
        for line in src_fp:
            if line.startswith("EOF"):
                break

            if line.startswith("#"):
                continue

            ip_prefix = line.split(column_delimiter)[4]

            if not is_foreign_network(ipcalc.Network(ip_prefix)):
                write_queue.put(line)

            line_count = line_count + 1

    execution_time = time.time() - start_time
    print("--- post processed %d networks in %s seconds ---" % (line_count, execution_time))
    write_queue.put("EOF --- post processed %d networks in %s seconds ---" % (line_count, execution_time))

    pool.close()


# multiprocessing.Queue -> None
def listen_for_final_record_write_request(queue):
    dest_filename = output_directory + file_base_name_output + "_post_" + file_date_for_post_processing + ".txt"

    with open(dest_filename, "w") as dest_fp:
        while True:
            message = queue.get()
            if message is "EOF":
                break
            dest_fp.write(str(message))
            dest_fp.flush()
        dest_fp.close()


# CONFIGURATION
now = datetime.datetime.now()
file_base_name_ending = "_" + str(now.month) + "_" + str(now.day) + "_" + str(now.year) + ".txt"

file_base_name_registry_data = ""
file_base_name_output = ""
file_base_name_output_failed_lookup = ""
file_base_name_output_exception = ""

registry_data_directory = "data/"
output_directory = ""

lines_to_process = 0
column_delimiter = ""
file_date_for_post_processing = ""

do_parse_task = True
do_post_process_task = True


def setup():
    config = configparser.ConfigParser()
    config.read('config.ini')

    # DIRECTORY SETTINGS
    global registry_data_directory
    registry_data_directory = config['DIRECTORIES']['RegistryDataDirectory']

    if registry_data_directory in ("None", ""):
        print "Please specify the directory for RIPE's DB snapshots."
        return False

    if not registry_data_directory.endswith('/'):
        registry_data_directory = registry_data_directory + "/"

    global output_directory
    output_directory = config['DIRECTORIES']['OutputDirectory']

    if output_directory in ("None", ""):
        print "Please specify the directory for produced output."
        return False

    if not output_directory.endswith('/'):
        output_directory = output_directory + "/"

    # OUTPUT SETTINGS
    global file_base_name_registry_data
    file_base_name_registry_data = config['OUTPUT']['FileBaseRegistryData']

    if file_base_name_registry_data in ("None", ""):
        file_base_name_registry_data = "ripe.db"

    global file_base_name_output
    file_base_name_output = config['OUTPUT']['FileBaseOutput']

    if file_base_name_output in ("None", ""):
        file_base_name_output = "ripe_registry_data"

    global file_base_name_output_failed_lookup
    file_base_name_output_failed_lookup = config['OUTPUT']['FileBaseFailedLookup']

    if file_base_name_output_failed_lookup in ("None", ""):
        file_base_name_output_failed_lookup = "ripe_registry_failed_organisation_lookups"

    global file_base_name_output_exception
    file_base_name_output_exception = config['OUTPUT']['FileBaseException']

    if file_base_name_output_exception in ("None", ""):
        file_base_name_output_exception = "ripe_registry_exceptions"

    # TASK SETTINGS
    global column_delimiter
    column_delimiter = config['TASK']['ColumnDelimiter']

    if column_delimiter in ("None", ""):
        print "Please specify column delimiter for output."
        return False

    if column_delimiter == "Unicode":
        column_delimiter = "\024"
    elif len(column_delimiter) > 1:
        print "No valid column delimiter character given."
        return False

    global lines_to_process
    lines_to_process = config['TASK']['LinesToProcess']

    if lines_to_process in ("None", ""):
        lines_to_process = 0
    else:
        lines_to_process = int(lines_to_process)

    global file_date_for_post_processing
    file_date_for_post_processing = config['TASK']['FileDateForPostProcess']

    if file_date_for_post_processing in ("None", ""):
        file_date_for_post_processing = file_base_name_ending[1:-4]

    global do_parse_task
    do_parse_task = config['TASK']['ParseTask']

    if do_parse_task in ("True", "False"):
        do_parse_task = bool(do_parse_task)
    else:
        do_parse_task = True

    global do_post_process_task
    do_post_process_task = config['TASK']['PostProcessTask']

    if do_post_process_task in ("True", "False"):
        do_post_process_task = bool(do_parse_task)
    else:
        do_post_process_task = False

    return True


# PROGRAM LOGIC
def main():
    ready = setup()

    if not ready:
        return

    if do_parse_task:
        import_ripe_registry_data()

    if do_post_process_task:
        if os.path.exists(output_directory + file_base_name_output + "_" + file_date_for_post_processing + ".txt"):
            post_process_ripe_registry_data()
        else:
            print "Specified file to be post-processed does not exist."


if __name__ == '__main__':
    main()
