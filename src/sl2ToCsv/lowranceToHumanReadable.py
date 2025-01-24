# Please take to note, that this script is only usable for coordinates in the northern hemisphere. 
# For the southern hemisphere (or negative lat/lng coordinates), the calculation of the coordinates has to be adjusted.
import struct
from bitarray import bitarray
import yaml
from math import exp, atan, pi
from datetime import datetime, timezone
import csv

class SL2Decoder:
    def __init__(self, filepath, config_path, verbose=True):
        self.filepath = filepath
        self.config_path = config_path
        self.verbose = verbose
        self.records = []
        self.POLAR_EARTH_RADIUS = 6356752.3142  #radius for the conversion of Spherical Mercator coordinates to WGS84 coordinates
        self.config = {
            # Default conversion factors
            "distance_conversion": 1.0,
            "speed_conversion": 1.0,
            "convert_coordinates": False,
            "include_raw": False,
        }
        self._load_config()

    def _load_config(self):
        """Lädt die Konfigurationswerte aus einer YAML-Datei."""
        with open(self.config_path, 'r') as file:
            config_data = yaml.safe_load(file)
            units = config_data.get("units", {})

            self.config["distance_conversion"] = 0.3048 if units.get("distance") == "meter" else 1.0
            self.config["speed_conversion"] = 1.852 if units.get("speed") == "kmh" else 1.0
            self.config["convert_coordinates"] = units.get("coordinates") == "wgs84"
            self.config["include_raw"] = units.get("include_raw", False)

    def decode(self):

        with open(self.filepath, 'rb') as f:
            data = f.read()

        pos = 0
        header = data[pos:pos + 10]
        pos += 10 # skip the header, sources are not clear about this value

        # Decode the header
        file_format = struct.unpack('<H', header[0:2])[0]

        if file_format not in range(1, 4):
            raise ValueError("Invalid 'format' in header; Likely not an SLG/SL2/SL3 file")

        block_size = struct.unpack('<H', header[4:6])[0]

        if block_size not in [1970, 3200]:
            raise ValueError("Block size is not 'downscan' or 'sidescan'; Likely not an SLG/SL2/SL3 file")
        
        time1_raw = struct.unpack('<I', data[pos + 60:pos + 64])[0] # seconds since 1980 (GPS Time)
        time1_utc = time1_raw - 315964800 # subtract 315964800 seconds to get time since 1970 (UTC)
        time1_iso = datetime.fromtimestamp(time1_utc, tz=timezone.utc)
        

        if self.verbose:
            print(f"Format: {['slg', 'sl2', 'sl3'][file_format - 1]}")
            print(f"Block size: {'downscan' if block_size == 1970 else 'sidescan'}")
            print(f"Time since 1970: {time1_utc}")
            print(f"Date and time of measurement: {time1_iso.isoformat()}")

        # Read the records
        while pos < len(data):
            if self.verbose and (len(self.records) % 100 == 0):
                print('.', end='', flush=True)
            try:
                block_size = struct.unpack('<H', data[pos + 28:pos + 30])[0]
                packet_size = struct.unpack('<H', data[pos + 34:pos + 36])[0]
                record = self._decode_record(data, pos, block_size)
                self.records.append(record)
                pos += packet_size + 144
            except (struct.error, IndexError) as e:
                print(f"Error decoding record: {e}")
                break
        
        if self.verbose:
            print("\nDecoding completed.")    
    
    def _decode_record(self, data, pos, block_size):
        """Dekodiert einen einzelnen Datenblock."""
        # Read raw values
        frame_offset = struct.unpack('<I', data[pos + 0:pos + 4])[0]
        prim_last_channel_frame_offset = struct.unpack('<I', data[pos + 4:pos + 8])[0]
        sec_last_channel_frame_offset = struct.unpack('<I', data[pos + 8:pos + 12])[0]
        downscan_last_channel_frame_offset = struct.unpack('<I', data[pos + 12:pos + 16])[0]
        side_left_last_channel_frame_offset= struct.unpack('<I', data[pos + 16:pos + 20])[0]
        side_right_last_channel_frame_offset = struct.unpack('<I', data[pos + 20:pos + 24])[0]
        composite_last_channel_frame_offset = struct.unpack('<I', data[pos + 24:pos + 28])[0]
        block_size = struct.unpack('<h', data[pos + 28:pos + 30])[0] #only needed for reefmaster (whole block size matters)
        last_block_size = struct.unpack('<h', data[pos + 30:pos + 32])[0] #only needed for reefmaster (whole block size matters)
        channel = struct.unpack('<h', data[pos + 32:pos + 34])[0] #only needed for reefmaster (whole block size matters)
        packet_size = struct.unpack('<h', data[pos + 34:pos + 36])[0] #only needed for reefmaster (whole block size matters)
        frame_index = struct.unpack('<i', data[pos + 36:pos + 40])[0] #only needed for reefmaster (whole block size matters)
        upper_limit_raw = struct.unpack('<f', data[pos + 44:pos + 48])[0]
        lower_limit_raw = struct.unpack('<f', data[pos + 48:pos + 52])[0]
        frequency = struct.unpack('<b', data[pos + 53:pos + 54])[0] #only needed for reefmaster (whole block size matters)
        time1_raw = struct.unpack('<I', data[pos + 60:pos + 64])[0] # seconds since 1980 (GPS Time) Warning! From this point all entries are 4 bytes further than in the documentation (https://wiki.openstreetmap.org/wiki/SL2)
        water_depth_raw = struct.unpack('<f', data[pos + 64:pos + 68])[0]
        keel_depth_raw = struct.unpack('<f', data[pos + 68:pos + 72])[0] 
        speed_gps_raw = struct.unpack('<f', data[pos + 100:pos + 104])[0]
        water_temperature = struct.unpack('<f', data[pos + 104:pos + 108])[0] #only needed for reefmaster (whole block size matters)
        lat_raw = struct.unpack('<i', data[pos + 108:pos + 112])[0]
        lng_raw = struct.unpack('<i', data[pos + 112:pos + 116])[0]
        speed_water_raw = struct.unpack('<f', data[pos + 116:pos + 120])[0] #only needed for reefmaster (whole block size matters)
        course_over_ground = struct.unpack('<f', data[pos + 120:pos + 124])[0] #only needed for reefmaster (whole block size matters)
        altitude = struct.unpack('<f', data[pos + 124:pos + 128])[0] #only needed for reefmaster (whole block size matters)
        heading = struct.unpack('<f', data[pos + 128:pos + 132])[0] #only needed for reefmaster (whole block size matters)
        flags_raw = struct.unpack('<H', data[pos + 132:pos + 134])[0]  # Bit-coded flags
        time_offset_raw = struct.unpack('<i', data[pos + 140:pos + 144])[0]

        
        # Conversion of raw values
        upper_limit = self._convert_distance(upper_limit_raw)
        lower_limit = self._convert_distance(lower_limit_raw)
        water_depth = self._convert_distance(water_depth_raw)
        speed_gps = self._convert_speed(speed_gps_raw)
        latitude, longitude = self._convert_coordinates(lat_raw, lng_raw)
        speed_water = self._convert_speed(speed_water_raw)
        time_offset = self._convert_time(time_offset_raw)


        sounding_data = self._extract_sounding_data(data[pos + 145:pos + 145 + block_size])


        # Select data fields to include in the output
        return {
            "frame_offset": frame_offset,
            "prim_last_channel_frame_offset": prim_last_channel_frame_offset,
            "sec_last_channel_frame_offset": sec_last_channel_frame_offset,
            "downscan_last_channel_frame_offset": downscan_last_channel_frame_offset,
            "side_left_last_channel_frame_offset": side_left_last_channel_frame_offset,
            "side_right_last_channel_frame_offset": side_right_last_channel_frame_offset,
            "composite_last_channel_frame_offset": composite_last_channel_frame_offset,
            "block_size": block_size,
            "last_block_size": last_block_size,
            "channel": channel,
            "packet_size": packet_size,
            "frame_index": frame_index,
            "upper_limit": upper_limit if not self.config["include_raw"] else upper_limit_raw,
            "lower_limit": lower_limit if not self.config["include_raw"] else lower_limit_raw,
            "frequency": frequency,
            "time1": time1_raw,
            "water_depth": water_depth if not self.config["include_raw"] else water_depth_raw,
            "keel_depth": keel_depth_raw,
            "speed_gps": speed_gps,
            "temperature": f"{water_temperature:.2f}",
            #"temperature": water_temperature,
            "latitude": latitude,
            "longitude": longitude,
            "speed_water": speed_water,
            "course_over_ground": f"{course_over_ground:.3f}",
            #"course_over_ground": course_over_ground,
            "altitude": f"{altitude:.3f}",
            #"altitude": altitude,
            "heading": f"{heading:.3f}",
            #"heading": heading,
            "flags": flags_raw,
            "time_offset": time_offset,  #  seconds since system startup
            #"sounding_data": sounding_data  # Decoded sounding data
            #here you can add more fields if needed or comment out fields wich are not needed, see the documentation for the full list of possible fields
    }    

    def clean_csv(self, input_path, output_path):
        valid_rows = []
        
        with open(input_path, 'r', newline='') as infile:
            reader = csv.DictReader(infile)
            headers = reader.fieldnames  # Spaltennamen speichern
            
            
            for row in reader:
                try:
                    block_size = int(row["block_size"])
                    last_block_size = int(row["last_block_size"])
                    packet_size = int(row["packet_size"])
                    
                    # Überprüfen, ob die Werte gültig sind
                    if block_size == 2064 and last_block_size == 2064 and packet_size == 1920:
                        valid_rows.append(row)  # Nur gültige Zeilen behalten

                except ValueError:
                    continue  # Falls Konvertierung fehlschlägt, Zeile ignorieren

        # Neue bereinigte CSV speichern
        with open(output_path, 'w', newline='') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=headers)
            writer.writeheader()
            writer.writerows(valid_rows)

    def _convert_speed(self, value):
        """Conversion of speed values (from knots to km/h)."""
        value_km_h = value * self.config["speed_conversion"]
        return f"{value_km_h:.2f}" # rounded to 2 decimal places
    
    def _convert_distance(self, value):
        """Conversion of distance values (from feet to meters)."""
        value_meters = value * self.config["distance_conversion"]
        return f"{value_meters:.3f}" # rounded to 3 decimal places
    
    def _convert_time(self, value):
        """Conversion of time values (from milliseconds to minutes)."""
        return f"{value / 1000:.4f}" # seconds since system start up to hundreth (ms)

    def _convert_coordinates(self, lng_raw, lat_raw):
        """Converts Spherical Mercator coordinates to WGS84 (Lat,Lng)."""
        if not self.config["convert_coordinates"]:
            return lng_raw, lat_raw

        longitude = lng_raw / self.POLAR_EARTH_RADIUS * (180 / pi)
        temp = lat_raw / self.POLAR_EARTH_RADIUS
        temp = exp(temp)
        latitude = (2 * atan(temp) - (pi / 2)) * (180 / pi)
        return f"{longitude:.8f}", f"{latitude:.8f}"       
        
    def _decode_channel(self, channel):
        channels = {
            0: "Primary",
            1: "Secondary",
            2: "DSI (Downscan)",
            3: "Left (Sidescan)",
            4: "Right (Sidescan)",
            5: "Composite"
        }
        return channels.get(channel, "Other/invalid")

    def _decode_frequency(self, frequency):
        frequencies = {
            0: "200 KHz",
            1: "50 KHz",
            2: "83 KHz",
            4: "800 KHz",
            5: "38 KHz",
            6: "28 KHz",
            7: "130-210 KHz",
            8: "90-150 KHz",
            9: "40-60 KHz",
            10: "25-45 KHz"
        }
        return frequencies.get(frequency, "Other/invalid")

    def _decode_flags(self, flag_bytes):
        bits = bitarray(endian='little')
        bits.frombytes(flag_bytes)
        flags = {
            "TrackValid": bits[0],
            "WaterSpeedValid": bits[1],
            "PositionValid": bits[3],
            "WaterTempValid": bits[5],
            "GpsSpeedValid": bits[6],
            "AltitudeValid": bits[14],
            "HeadingValid": bits[15]
        }
        return flags


    def _extract_sounding_data(self, data_block):
    # Convert raw sounding data block into a list of values
        return list(data_block)

    def save_to_csv(self, output_path):
        """Speichert die dekodierten Daten als CSV mit festen Sounding-Spalten."""
        if not self.records:
            print("Keine Daten zum Speichern vorhanden!")
            return

        # Basis-Header definieren (alle Spalten ohne Sounding-Daten)
        base_headers = list(self.records[0].keys())

        # Prüfen, ob `sounding_data` existiert, dann feste Spaltennamen für jedes sounding_data byte erzeugen
        if "sounding_data" in base_headers:
            packet_size = 1920  # Anzahl der Sounding-Werte pro Block (fest definiert)
            sounding_headers = [f"sounding_{i+1}" for i in range(packet_size)]
            base_headers.remove("sounding_data")  # `sounding_data`-Key aus der Basis-Headerliste entfernen
            headers = base_headers + sounding_headers  # Sounding-Spalten anhängen
        else:
            headers = base_headers  # Falls keine Sounding-Daten existieren, bleibt der Header normal

        # CSV speichern
        with open(output_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()

            for record in self.records:
                row = record.copy()
                if "sounding_data" in record:
                    sounding_values = record["sounding_data"]

                    # Falls weniger als 1920 Werte: Mit None auffüllen
                    if len(sounding_values) < packet_size:
                        sounding_values.extend([None] * (packet_size - len(sounding_values)))

                    # Sounding-Werte in die festgelegten Spalten eintragen
                    for i in range(packet_size):
                        row[f"sounding_{i+1}"] = sounding_values[i]

                    # `sounding_data`-Key entfernen, da es nun in Spalten aufgeteilt wurde
                    del row["sounding_data"]

                writer.writerow(row)

# Example usage
# decoder = SL2Decoder('path_to_file.sl2')
# decoder.decode()
# decoder.save_to_csv('output.csv')
# from https://wiki.openstreetmap.org/wiki/SL2 and https://gitlab.com/hrbrmstr/arabia


filepath = r"C:\Users\ssteinhauser\Masterthesis\LowFake\Decoder\Chart 05_11_2018 [0].sl2"
#filepath = r"C:\Users\ssteinhauser\Masterthesis\LowFake\Decoder\Chart 03_08_2005 [0].sl2"
config_path = r"C:\Users\ssteinhauser\Masterthesis\LowFake\Decoder\lowFakeConfig.yaml"
csv_path = r"C:\Users\ssteinhauser\Masterthesis\LowFake\Decoder\sl2ToCsvOutput_raw.csv"
csv_path_cleaned = r"C:\Users\ssteinhauser\Masterthesis\LowFake\Decoder\sl2ToCsvOutput.csv" #cleanded path is the new default

decoder = SL2Decoder(filepath, config_path, verbose=True)
decoder.decode()
decoder.save_to_csv(csv_path)
decoder.clean_csv(csv_path,csv_path_cleaned)