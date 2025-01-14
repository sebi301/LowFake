# Please take to note, that this script is only usable for koordinate in the norther hemisphere. 
# For the southern hemisphere the calculation of the coordinates has to be adjusted.
import struct
from bitarray import bitarray
import yaml
from math import exp, atan, pi
from datetime import datetime, timezone, timedelta
from datetime import datetime, timezone





class SL2Decoder:
    def __init__(self, filepath, config_path, verbose=True):
        self.filepath = filepath
        self.config_path = config_path
        self.verbose = verbose
        self.records = []
        self.POLAR_EARTH_RADIUS = 6356752.3142  # Radius für die Umrechnung von Spherical Mercator-Koordinaten in WGS84-Koordinaten
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
        pos += 10 #Hier sind die Quellen sich nicht einig. Manche schalgen Pos 8 oder 10 vor.

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
        # Rohdaten auslesen
        # hier sind alle Einträge 4 Bytes weiter als in der Doku
        time1_raw = struct.unpack('<I', data[pos + 60:pos + 64])[0] # seconds since 1980 (GPS Time)
        upper_limit_raw = struct.unpack('<f', data[pos + 44:pos + 48])[0]
        lower_limit_raw = struct.unpack('<f', data[pos + 48:pos + 52])[0]
        water_depth_raw = struct.unpack('<f', data[pos + 64:pos + 68])[0] 
        speed_gps_raw = struct.unpack('<f', data[pos + 100:pos + 104])[0]
        lat_raw = struct.unpack('<i', data[pos + 108:pos + 112])[0]
        lng_raw = struct.unpack('<i', data[pos + 112:pos + 116])[0]
        speed_water_raw = struct.unpack('<f', data[pos + 116:pos + 120])[0]
        time_offset_raw = struct.unpack('<i', data[pos + 140:pos + 144])[0]

        # Konvertierte Werte berechnen
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
            #"upper_limit": upper_limit if not self.config["include_raw"] else upper_limit_raw,
            #"lower_limit": lower_limit if not self.config["include_raw"] else lower_limit_raw,
            #"time1": time1_raw,
            "water_depth": water_depth if not self.config["include_raw"] else water_depth_raw,
            #"speed_gps": speed_gps,
            "latitude": latitude,
            "longitude": longitude,
            "speed_water": speed_water,
            "time_offset": time_offset_raw,
            "sounding_data": sounding_data
            #here we can add more fields if needed, see the documentation for the full list of possible fields
        }    

    def _convert_speed(self, value):
        """Konvertiert Geschwindigkeitswerte (z. B. Knoten in km/h)."""
        value_km_h = value * self.config["speed_conversion"]
        return f"{value_km_h:.2f}" # rounded to 2 decimal places
    
    def _convert_distance(self, value):
        """Konvertiert Entfernungswerte (z. B. Fuß in Meter)."""
        value_meters = value * self.config["distance_conversion"]
        return f"{value_meters:.3f}" # rounded to 3 decimal places
    
    def _convert_time(self, value):
        """Konvertiert Zeitwerte (z. B. Sekunden in Stunden)."""
        return f"{value / 1000:.2f}"

    def _convert_coordinates(self, lng_raw, lat_raw):
        """Konvertiert Spherical Mercator-Koordinaten zu WGS84."""
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
        with open(output_path, 'w') as f:
            headers = self.records[0].keys()
            f.write(','.join(headers) + '\n')
            for record in self.records:
                row = ','.join(str(record[h]) for h in headers)
                f.write(row + '\n')               

# Example usage
# decoder = SL2Decoder('path_to_file.sl2')
# decoder.decode()
# decoder.save_to_csv('output.csv')
# from https://wiki.openstreetmap.org/wiki/SL2 and https://gitlab.com/hrbrmstr/arabia


filepath = r"C:\Users\ssteinhauser\Masterthesis\LowFake\Temp\Chart 03_08_2005 [0].sl2"
config_path = r"C:\Users\ssteinhauser\Masterthesis\LowFake\Temp\lowFakeConfig.yaml"


decoder = SL2Decoder(filepath, config_path, verbose=True)
decoder.decode()
decoder.save_to_csv(r'c:\Users\ssteinhauser\Masterthesis\LowFake\Tempsl2ToCsvOutput.csv')