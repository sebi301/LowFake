import csv
from datetime import datetime, timedelta, timezone
import os
import utm
from tqdm.auto import tqdm
from multiprocessing import Pool, cpu_count

# Helper functions
def gps_weeks_to_millis(weeks):
    """Convert GPS weeks to milliseconds."""
    return weeks * 7 * 86400 * 1000


def round_to_nearest_200ms_modulo(timestamp):
    """Round a timestamp to the nearest 200ms using modulo."""
    remainder = timestamp % 200
    if remainder >= 100:
        return timestamp + (200 - remainder)  # Round up
    else:
        return timestamp - remainder  # Round down
    

def convert_gps_to_unix_modulo(gms, gwk):
    """Convert GPS time to Unix time and round using modulo method."""
    millis_between_unix_and_gps = 315964800 * 1000
    total_millis = gps_weeks_to_millis(gwk) + gms
    unix_timestamp = total_millis + millis_between_unix_and_gps
    return round_to_nearest_200ms_modulo(unix_timestamp)


def interpolate_gps_and_metrics(before, after, target_timestamp):
    """Interpolate GPS coordinates and additional metrics (Spd_kmh, delta_s) for a target timestamp."""
    timestamp_before = int(before[0])  # Unix timestamp before the target
    timestamp_after = int(after[0])    # Unix timestamp after the target
    
    # Calculate the interpolation factor
    factor = (target_timestamp - timestamp_before) / (timestamp_after - timestamp_before)
    
    # Extract data for interpolation
    lat_before, lon_before = float(before[1][8]), float(before[1][9])
    lat_after, lon_after = float(after[1][8]), float(after[1][9])
    spd_kmh_before, delta_s_before = float(before[1][18]), float(before[1][19])
    spd_kmh_after, delta_s_after = float(after[1][18]), float(after[1][19])
    
    # Interpolate latitude, longitude, Spd_kmh, and delta_s
    interpolated_lat = lat_before + factor * (lat_after - lat_before)
    interpolated_lon = lon_before + factor * (lon_after - lon_before)
    interpolated_spd_kmh = spd_kmh_before + factor * (spd_kmh_after - spd_kmh_before)
    interpolated_delta_s = delta_s_before + factor * (delta_s_after - delta_s_before)
    
    return interpolated_lat, interpolated_lon, interpolated_spd_kmh, interpolated_delta_s



def find_nearest_gps_entries_fixed_interval(gps_data, target_timestamp, reference_timestamp, interval_ms=200):
    """Find the nearest GPS entries in an array with a fixed interval by calculating position directly."""
    # Round the timestamp to the next multiple of interval_ms
    lower_bound_gps_timestamp = (target_timestamp // interval_ms) * interval_ms
    upper_bound_gps_timestamp = lower_bound_gps_timestamp + interval_ms

    # Search with lower_bound_gps_timestamp and upper_bound_gps_timestamp after the fitting gps rows
    before = None
    after = None

    for row in gps_data:
        timestamp = int(row[0])
        if timestamp == lower_bound_gps_timestamp:
            before = row
        elif timestamp == upper_bound_gps_timestamp:
            after = row
        if before and after:
            break 

    if before is None or after is None:
        # Ziel-Zeitstempel liegt außerhalb des GPS-Bereichs
        return None, None
    else:
        return before, after


def process_rows_for_matching(args):
    """Process rows with GPS data matching and adding Spd_kmh, delta_s."""
    synched_rows, gps_file_path, utc_local_shift, convert_to_utm, method = args
    matched_data = []

    # Lade GPS-Daten einmal in den Speicher, Header-bezogen
    gps_data = []
    transect_data = {}
    with open(gps_file_path, 'r') as gps_file:
        reader = csv.reader(gps_file)
        headers = next(reader)  # Header einlesen

        # Bestimme Spaltenindizes durch Headernamen
        timestamp_index = headers.index("UnixTimestamp")
        lat_index = headers.index("Lat")
        lon_index = headers.index("Lng")
        spd_kmh_index = headers.index("Spd_kmh")
        delta_s_index = headers.index("delta_s")

        for row in reader:
            try:
                gps_timestamp = int(row[timestamp_index])  # Unix-Timestamp
                spd_kmh = row[spd_kmh_index]              # Spd_kmh
                delta_s = row[delta_s_index]              # delta_s
                gps_data.append((gps_timestamp, row))     # Für "smallestDifference"
                transect_data[gps_timestamp] = (spd_kmh, delta_s)  # Für Lookup
            except (IndexError, ValueError):
                continue

    # Referenz-Zeitstempel ist der erste Eintrag
    reference_timestamp = gps_data[0][0]

    for synched_row in synched_rows:
        try:
            target_timestamp = int(synched_row[0])  # Ziel-Zeitstempel

            if method == "smallestDifference":
                # Finde den nächsten GPS-Wert
                closest_gps_row = min(gps_data, key=lambda x: abs(x[0] - target_timestamp))
                closest_timestamp, gps_row = closest_gps_row
                time_difference = closest_timestamp - target_timestamp
                lat = float(closest_gps_row[8])
                lon = float(closest_gps_row[9])

                # Extrahiere Spd_kmh und delta_s
                spd_kmh, delta_s = transect_data.get(closest_timestamp, ("", ""))

                # Erstelle die neue Zeile
                new_row = synched_row[:3] + [
                    closest_timestamp, time_difference, 
                    gps_row[lat_index], gps_row[lon_index],
                    spd_kmh, delta_s
                ] + synched_row[3:]

                if convert_to_utm:
                    utm_x, utm_y, zone_number, zone_letter = utm.from_latlon(lat, lon)
                    new_row = new_row[:9] + [round(utm_x*1000), round(utm_y*1000), zone_number, zone_letter] + new_row[9:]
            
            
            elif method == "interpolate":
                # Finde die zwei nächsten GPS-Einträge für die Interpolation
                before, after = find_nearest_gps_entries_fixed_interval(gps_data, target_timestamp, reference_timestamp)
                if before is None or after is None:
                    continue  # Überspringe, falls keine beiden Werte gefunden wurden
                
                # Interpoliere die GPS-Koordinaten und Metriken
                interpolated_lat, interpolated_lon, interpolated_spd_kmh, interpolated_delta_s = interpolate_gps_and_metrics(before, after, target_timestamp)


                # Werte von 'before' für Spd_kmh und delta_s verwenden (als Näherung)
                spd_kmh, delta_s = transect_data.get(before[0], ("", ""))

                # Erstelle die neue Zeile
                new_row = synched_row[:3] + [
                    target_timestamp, 0, 
                    round(interpolated_lat,7), round(interpolated_lon,7),
                    round(interpolated_spd_kmh, 5), round(interpolated_delta_s,5)
                ] + synched_row[3:]

            # Optional: UTM-Koordinaten hinzufügen
            if convert_to_utm:
                utm_x, utm_y, zone_number, zone_letter = utm.from_latlon(interpolated_lat,interpolated_lon)
                new_row = new_row[:9] + [round(utm_x*1000), round(utm_y*1000), zone_number, zone_letter] + new_row[9:] # Converts UTM coordinates to millimeters (integer) for calculations without decimal points

            matched_data.append(new_row)
        except (IndexError, ValueError):
            continue

    return matched_data


# Functions for processing data

# Step 1: Processing GPS Log Data
    
def process_gps_log(input_file_path, output_file_path, utc_local_shift=None):
    """Process GPS log data, add Unix and local timestamps (in UTC+2), and save to output file."""
    with open(input_file_path, 'r') as infile, open(output_file_path, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(["Type", "TimeUS", "Instance", "Status", "GMS", "GWk", "NSats", "HDop", "Lat", "Lng", "Alt", "Spd", "GCrs", "VZ", "Yaw", "U", "UnixTimestamp", f"UTC+{utc_local_shift}Local", "Spd_kmh", "delta_s"])

        spd = 0
        last_unix_timestamp = 0
        last_spd = 0

        for line in tqdm(infile, desc="Step 1: Processing GPS Data:"):
            if line.startswith("GPS"):
                fields = line.strip().split(",")
                try:
                    gms = int(fields[4])
                    gwk = int(fields[5])
                    spd = float(fields[11]) #Velocity in m/s
                    unix_timestamp = convert_gps_to_unix_modulo(gms, gwk)
                    
                    # Create timezone-aware UTC datetime, then convert to UTC+2
                    utc_datetime = datetime.fromtimestamp(unix_timestamp / 1000, timezone.utc)
                    local_timestamp = utc_datetime + timedelta(hours=utc_local_shift)
                    
                    # Calculate delta_s (Delta Distance)
                    delta_s = 0  # Default value in meters
                    if last_unix_timestamp is not None and last_spd is not None:
                        delta_t = (unix_timestamp - last_unix_timestamp) / 1000  # Convert ms to s
                        if delta_t > 0:  # Avoid division by zero
                            delta_s = round(spd * delta_t, 5) # 5: round to four decimal places to prevent roundoff error
                    else:
                        delta_t = 0  # Initialwert für die erste Zeile

                    # Update last values
                    last_unix_timestamp = unix_timestamp
                    last_spd = spd
                    
                    spd_kmh = round(spd * 3.6, 5) # 3.6: converting factor ; 5: round to four decimal places to prevent roundoff error

                    writer.writerow(fields + [unix_timestamp, local_timestamp.strftime('%Y-%m-%d %H:%M:%S'),spd_kmh,delta_s])
                except (IndexError, ValueError) as e:
                    print(f"Skipping line due to error: {e}, Line: {fields}")



# Step 2: Filtering and Synchronizing Deeper and Sonar Data
def filter_gps_points(input_path, output_path):
    """Filter GPS points with 0.0 latitude and longitude."""
    with open(input_path, 'r') as infile, open(output_path, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        for row in tqdm(csv.reader(infile), desc="Step 2: Filter Bathymetry Data:"):
            for row in csv.reader(infile):
             if len(row) > 1 and row[0] == '0.0' and row[1] == '0.0':
                writer.writerow(row)
                

def synchronize_data(filtered_path, sonar_path, output_path, utc_local_shift=2):
    """Synchronize Deeper and Sonar data without needing headers."""
    with open(filtered_path, 'r') as filtered_file, open(sonar_path, 'r') as sonar_file, open(output_path, 'w', newline='') as output_file:
        writer = csv.writer(output_file)
        filtered_reader = csv.reader(filtered_file)
        sonar_reader = csv.reader(sonar_file)

        has_data = False  # Track if any data rows are written

        # Iterate over both files simultaneously
        for filtered_row, sonar_row in tqdm( zip(filtered_reader, sonar_reader), desc="Step 3: Synchronizing Bathymetry and Sonar:"):
            try:
                # Check if rows are of expected lengths
                if len(filtered_row) < 3 or len(sonar_row) < 2:
                    print("Skipping row due to insufficient columns:", filtered_row, sonar_row)
                    continue

                # Parse timestamps and generate output row
                unix_timestamp = int(filtered_row[-1])
                local_timestamp = datetime.fromtimestamp(unix_timestamp / 1000, timezone.utc) + timedelta(hours=utc_local_shift)

                writer.writerow([unix_timestamp, filtered_row[2], local_timestamp.strftime('%Y-%m-%d %H:%M:%S')] + sonar_row[1:])
                has_data = True  # Mark that we've written data
            except (IndexError, ValueError) as e:
                print(f"Skipping row due to error: {e}, Filtered Row: {filtered_row}, Sonar Row: {sonar_row}")

        if not has_data:
            print("No data was written to output in synchronize_data.")                

def create_column_names(input_filtered_path, final_output_path):
    """Create column names and write to an output file."""

    # Define the initial header
    header = ["UnixTimestamp", "Depth", f"UTC+2Local"]

    # Read the input file to determine the number of columns in the longest row
    max_columns = 0
    with open(input_filtered_path, 'r') as infile:
        reader = csv.reader(infile)
        for row in tqdm(reader, desc="Step 4: Determining Column Names:"):
            if len(row) > max_columns:
                max_columns = len(row)

    # Generate the additional column names with time intervals based on the longest row
    num_additional_columns = max_columns - 3
    if num_additional_columns > 0:
        additional_columns = [f"{0.010407008 + i * 0.010407008:.9f}" for i in range(num_additional_columns)]
        header.extend(additional_columns)

    # Reopen the file to write the updated headers and data
    with open(input_filtered_path, 'r') as infile, open(final_output_path, 'w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        # Write the new header
        writer.writerow(header)
        
        # Write the data rows
        writer.writerows(reader)


# Step 3: Synchronize GPS and Synched Deeper Data
def find_closest_gps_timestamp(gps_file_path, target_timestamp):
    closest_row = None
    min_time_diff = float('inf')
    with open(gps_file_path, 'r') as gps_file:
        reader = csv.reader(gps_file)
        next(reader)
        for row in reader:
            try:
                gps_timestamp = int(row[-2])
                time_diff = abs(gps_timestamp - target_timestamp)
                if time_diff < min_time_diff:
                    min_time_diff = time_diff
                    closest_row = row
            except (IndexError, ValueError) as e:
                print(f"Skipping GPS row due to error: {e}, Row: {row}")
    return closest_row


def remove_duplicate_timestamps(file_path):
    """
    Delets all rows with non-unique Unix timestamps (except the first occurrence).

    Parameters:
    - file_path (str): Path to the CSV file.
    """
    try:
        seen_timestamps = set()
        unique_rows = []

        # Datei einlesen und doppelte Einträge entfernen
        with open(file_path, mode='r') as file:
            reader = csv.reader(file)
            header = next(reader)  # Kopfzeile extrahieren
            unique_rows.append(header)

            for row in reader:
                if len(row) > 0:
                    timestamp = row[0]  # Erster Eintrag in der Zeile (Unix-Zeitstempel)
                    if timestamp not in seen_timestamps:
                        seen_timestamps.add(timestamp)
                        unique_rows.append(row)

        # Datei überschreiben mit den eindeutigen Zeilen
        with open(file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(unique_rows)

        print(f"Step 3.1: All rows with non-unique timestamps have been removed. Cleaned file saved: {file_path}")
    except Exception as e:
        print(f"Error processing the file: {e}")



def match_gps_with_synched_data_parallel(input_path, gps_file_path, output_path, convert_to_utm=False, num_processes=None, method=None):
    if num_processes is None:
        num_processes = cpu_count()  # Use the available CPU cores
    
    with open(input_path, 'r') as infile:
        synched_reader = csv.reader(infile)
        header = next(synched_reader)

        # Insert new headers as per requirements
        header.insert(3, "GPSUnixTimestamp")
        header.insert(4, "TimeDifference(ms)")
        header.insert(5, "Latitude")
        header.insert(6, "Longitude")
        header.insert(7, "Spd_kmh")
        header.insert(8, "delta_s")
        
        if convert_to_utm:
            header.insert(9, "UTM_X_Easting") # UTM in millimeters
            header.insert(10, "UTM_Y_Northing") # UTM in millimeters
            header.insert(11, "UTM_Zone_Number")
            header.insert(12, "UTM_Zone_Letter")

        # Split synched data into chunks for parallel processing
        synched_data_chunks = []
        chunk_size = 25  # Adjust chunk size based on data size and memory capacity
        current_chunk = []

        for row in synched_reader:
            current_chunk.append(row)
            if len(current_chunk) >= chunk_size:
                synched_data_chunks.append((current_chunk, gps_file_path, 2, convert_to_utm, method))
                current_chunk = []

        if current_chunk:
            synched_data_chunks.append((current_chunk, gps_file_path, 2, convert_to_utm, method))

    # Use multiprocessing to process each chunk
    with Pool(processes=num_processes) as pool:
        results = list(tqdm(pool.imap(process_rows_for_matching, synched_data_chunks), total=len(synched_data_chunks), desc="Step 5: Matching GPS with Synched Data (Multiprocessing)"))

    # Write the combined results to the output file
    with open(output_path, 'w', newline='') as output_file:
        writer = csv.writer(output_file)
        writer.writerow(header)  # Write header

        for matched_rows in results:
            writer.writerows(matched_rows)  # Write each processed chunk


def delete_temp_files(temp_files):
    """Delete specified intermediate files if they exist."""

    for temp_file in temp_files:
        if os.path.exists(temp_file):
            os.remove(temp_file)  


def remove_columns_by_header(file_path, headers_to_remove):
    """
    Removes columns from a CSV file based on header names.
    
    Parameters:
    - file_path (str): Path to the CSV file.
    - headers_to_remove (list): List of header names to remove.
    """
    try:
        # Read the file
        with open(file_path, mode='r') as file:
            reader = csv.reader(file)
            headers = next(reader)  # Extract the header row

            # Find indices of columns to remove
            indices_to_remove = [headers.index(header) for header in headers_to_remove if header in headers]

            # Create a new header without the columns to remove
            new_headers = [header for i, header in enumerate(headers) if i not in indices_to_remove]

            # Filter rows to exclude the columns
            filtered_rows = [
                [value for i, value in enumerate(row) if i not in indices_to_remove]
                for row in reader
            ]

        # Overwrite the file with the filtered data
        with open(file_path, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(new_headers)  # Write the new header
            writer.writerows(filtered_rows)  # Write the filtered rows

        print(f"The following columns were removed: {headers_to_remove} from the file: {file_path}")

    except ValueError as ve:
        print(f"Error: One or more columns were not found: {ve}")
    except Exception as e:
        print(f"Error processing the file: {e}")


# Modify main function to use parallel matching for Step 5
def main():
    folder_path = r'C:\Users\ssteinhauser\Masterthesis\Sonar3DReconstruction\Temp'
    logfile_number = '00000016'
    log_file_name = f'{logfile_number}.log'
    input_file_path = os.path.join(folder_path, log_file_name)

    # 1. Process GPS log data
    output_file_name_unix = f'{logfile_number}_filtered_with_Unix.csv'
    output_file_path_unix = os.path.join(folder_path, output_file_name_unix)
    process_gps_log(input_file_path, output_file_path_unix, utc_local_shift=2)

    # 2. Filter GPS points
    input_path_bathymetry = os.path.join(folder_path, 'bathymetry.csv')
    input_path_sonar = os.path.join(folder_path, 'sonar.csv')
    output_path_filtered_firstStage = os.path.join(folder_path, 'synchedDeeperDataFirstStage.csv')
    filter_gps_points(input_path_bathymetry, output_path_filtered_firstStage)

    # 3. Synchronize filtered Bathymetry and Sonar data
    output_path_filtered_secondStage = os.path.join(folder_path, 'synchedDeeperDataSecondStage.csv')
    synchronize_data(output_path_filtered_firstStage, input_path_sonar, output_path_filtered_secondStage)
    if os.path.getsize(output_path_filtered_secondStage) == 0:
        print("Warning: Synchronize data step produced an empty output. Verify sonar.csv and bathymetry.csv data.")
        return  # Stop if empty
    
   # 3.1 Remove duplicate timestamps (optional)
    remove_duplicate_timestamps(output_path_filtered_secondStage)

    # 4. Create column names for the output file
    output_path_filtered_thirdStage = os.path.join(folder_path, 'synchedDeeperDataThirdStage.csv')
    create_column_names(output_path_filtered_secondStage, output_path_filtered_thirdStage)

    # 5. Match GPS data with synched Deeper data using multiprocessing
    final_output_file = os.path.join(folder_path, 'synchedDeeperData.csv')
    match_gps_with_synched_data_parallel(output_path_filtered_thirdStage, output_file_path_unix, final_output_file, convert_to_utm=False, num_processes=None, method="smallestDifference")

    # 6. Delete intermediate files
    temp_files = [
        output_file_path_unix,
        #output_path_filtered_firstStage,
        #output_path_filtered_secondStage,
        #output_path_filtered_thirdStage
    ]
    delete_temp_files(temp_files)

    # 7. Remove specefic rows whiche are not needed (optional)
    headers_to_remove = ["TimeDifference(ms)", "GPSUnixTimestamp"]  # Example: List of header names to remove
    remove_columns_by_header(final_output_file, headers_to_remove)


    print("All steps have been completed successfully.")

if __name__ == '__main__':
    main()
