# -- IMPORT -- #
# NON-CONCURRENT IMPORT
# None -> None
def import_registry_data_linear():
    line_count = 0
    object_count = -1

    temp_object = get_empty_inetnum_object()
    dest_filename = output_directory + file_base_name_output_linear + file_base_name_ending
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


# CONCURRENT MULTI-THREADED IMPORT
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


# String, multiprocessing.Queue -> None
def process_record_string(record, write_queue):
    processed_record = evaluate_inetnum_object(get_inetnum_object(record))
    write_queue.put(processed_record)
    return


if line.startswith(target_ripe_inetnum_attributes[0] + ":"):
    jobs.append(pool.apply_async(process_record_position,
                                 [
                                     next_line_byte_position,
                                     write_queue,
                                     failed_organisation_lookup_write_queue,
                                     exceptions_write_queue
                                 ]))
    # record = line + ''.join(islice(src_fp, 10))
    # jobs.append(pool.apply_async(process_record_string, [record, write_queue]))

if inetnum_key is "inetnum":
    inetnum_value = start_ip + column_delimiter + end_ip + column_delimiter + ip_prefix

    route_info = get_route_info(str(ipcalc.IP(start_ip)))

    if route_info is not None:
        for route_key, route_value in route_info.iteritems():
            if route_key is not "route":
                route_values = route_values + '"' + str(route_value) + '"' + column_delimiter
    else:
        route_values = "NULL" + column_delimiter + "NULL" + column_delimiter


# String -> Dict/None
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


# CONFIG
file_base_name_output_tmp = "ripe_registry"
file_base_name_output_linear = "ripe_registry_linear"
