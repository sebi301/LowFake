import struct
import csv

class SL2Encoder:
    def __init__(self, csv_filepath, sl2_filepath):
        self.csv_filepath = csv_filepath
        self.sl2_filepath = sl2_filepath
        self.records = []
        self.block_size = 144  # Minimum Blockgröße, anpassen bei mehr Daten

    def load_csv(self):
        """Lädt die CSV-Datei und speichert die Datensätze in self.records."""
        with open(self.csv_filepath, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if self._is_valid_row(row):
                    self.records.append(row)

    def _is_valid_row(self, row):
        """Prüft, ob eine Zeile valide ist."""
        # Prüfen, ob die Spalten `sounding_data` oder `0.010407008` existieren
        if '0.010407008' in row:
            # Wenn `0.010407008` existiert, ist die Zeile immer gültig
            return True
        elif 'sounding_data' in row:
            # Validierung für `sounding_data`
            sounding_data = row.get('sounding_data', '').strip()  # Leere Strings abfangen
            if not sounding_data or sounding_data == '[]':  # Leere Listen oder leere Felder
                return False

            # Zusätzlich prüfen, ob `water_depth` valide ist
            try:
                water_depth = float(row.get('water_depth', '0'))
                if water_depth > 500 or water_depth <= 0.3:  # Tiefe größer als 500, 0 oder negativ (500 weil 500feet möglich sind und das ist größer als 150m)
                    return False
                
                speed_gps = float(row.get('speed_gps', '0'))
                if speed_gps >100 or speed_gps < 0:  # Geschwindigkeit größer als 100 oder negativ (100 weil Knoten als mögliche Einheit sind und das ist größer als 180km/h)
                    return False

                block_size = int(row.get('block_size', '0'))
                if block_size > 2064:
                    return False
            except ValueError:
                return False

            return True
        # Wenn keine der beiden Spalten existiert, ist die Zeile ungültig
        return False
    
    def encode(self):
        """Kodiert die Daten und schreibt sie in eine SL2-Datei."""
        with open(self.sl2_filepath, 'wb') as f:
            # Schreibe den Header einer sl2 Downscan-Datei (02 00 00 00 b2 07 00 00 08 00) einmalig
            f.write(struct.pack('<IIH', 2, 1970, 8))

            # Schreibe die reslichen Blöcke
            for i, record in enumerate(self.records):
                block = self._create_block(record, i)
                f.write(block)

    def _create_block(self, record, index):
        """Erstellt einen einzelnen Block basierend auf einem Datensatz."""
        # Beispielwerte, angepasst basierend auf den Dokumentationen
        unknownPart1 = bytes([0x18, 0x62, 0x2F, 0x01, 0x18, 0x62, 0x2F, 0x01, 0x00, 0x00, 0x00, 0x00, 0x08, 0x5A, 0x2F, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]) #fake byte from real data alternative: bytes([0x01]*28)
        block_size = int(record['block_size'])
        last_block_size = int(record['last_block_size'])
        channel = int(record['channel'])
        packet_size = int(record['packet_size'])
        frame_index = int(record['frame_index'])
        upper_limit = float(record['upper_limit'])
        lower_limit = float(record['lower_limit'])
        unknownPart2 = bytes([0x00, 0x00, 0x00, 0x00, 0x00])#bytes([0x02]*5)
        frequency = int(float(record['frequency']))
        unknownPart3 = bytes([0x00]*6)
        time1 = int(record['time1'])
        water_depth = float(record['water_depth'])
        unknownPart4 = bytes([0x04]*32)
        speed_gps = float(record['speed_gps'])
        temperature = float(record['temperature'])
        latitude = float(record['latitude'])
        longitude = float(record['longitude'])
        speed_water = float(record['speed_water'])
        course_over_ground = float(record['course_over_ground'])
        altitude = float(record['altitude'])
        heading = float(record['heading'])
        flags = int(record['flags'])
        unknowPart5 = bytes([0x00, 0x00, 0x03, 0x03, 0x0A, 0x01]) #bytes([0x05]*6)
        time_offset = int(float(record['time_offset']))
        sounding_data = self._encode_sounding_data(record['sounding_data'])

        
        print(f"block_size={block_size}, last_block_size={last_block_size}, channel={channel}, "
        f"packet_size={packet_size}, frame_index={frame_index}, upper_limit={upper_limit}, "
        f"lower_limit={lower_limit}, frequency={frequency}, time1={time1}, water_depth={water_depth}, "
        f"speed_gps={speed_gps}, temprature={temperature}, latitude={latitude}, longitude={longitude}, "
        f"speed_water={speed_water}, course_over_ground={course_over_ground}, altitude={altitude}, "
        f"heading={heading}, flags={flags}, time_offset={time_offset}")
        
        # Blockstruktur erstellen
        block = struct.pack(
            '<28shhhhiff5sb2sif32sffffffffh6sh6s',
            unknownPart1,      # Unbekannter Teil 1 mit 0x01, Länge: 28 Bytes, Offset: 0
            block_size,        # Blockgröße, Länge: 2 Bytes (short), Offset: 28
            last_block_size,   # Letzte Blockgröße, Länge: 2 Bytes (short), Offset: 30
            channel,           # Kanal, Länge: 2 Bytes (short), Offset: 32
            packet_size,       # Paketgröße, Länge: 2 Bytes (short), Offset: 34
            frame_index,       # Frameindex, Länge: 4 Bytes (int), Offset: 36
            upper_limit,       # Oberes Limit, Länge: 4 Bytes (float), Offset: 40
            lower_limit,       # Unteres Limit, Länge: 4 Bytes (float), Offset: 44
            unknownPart2,      # Unbekannter Teil 2 mit 0x02, Länge: 5 Bytes, Offset: 48
            frequency,         # Frequenz, Länge: 1 Byte (signed char), Offset: 53
            unknownPart3,      # Unbekannter Teil 3 mit 0x03, Länge: 6 Bytes, Offset: 54
            time1,             # Zeit (ms seit Boot), Länge: 4 Bytes (int), Offset: 60
            water_depth,       # Wassertiefe, Länge: 4 Bytes (float), Offset: 64
            unknownPart4,      # Unbekannter Teil 4 mit 0x04, Länge: 32 Bytes, Offset: 68
            speed_gps,         # Geschwindigkeit, Länge: 4 Bytes (float), Offset: 100
            temperature,       # Temperatur, Länge: 4 Bytes (float), Offset: 104
            latitude,          # Breite, Länge: 4 Bytes (float eigentlich int), Offset: 108 TODO: In Spherical Mercator Projection umrechenen und dann in int umwandeln (nicht relevant bei raw data)
            longitude,         # Länge, Länge: 4 Bytes (float eigentlich int), Offset: 112 TODO: In Spherical Mercator Projection umrechenen und dann in int umwandeln (nicht relevant bei raw data)
            speed_water,       # Geschwindigkeit Wasser, Länge: 4 Bytes (float), Offset: 116
            course_over_ground,# Kurs über Grund, Länge: 4 Bytes (float), Offset: 120
            altitude,          # Höhe, Länge: 4 Bytes (float), Offset: 124
            heading,           # Heading, Länge: 4 Bytes (float), Offset: 128
            flags,             # Flags, Länge: 2 Bytes (short), Offset: 132
            unknowPart5,       # Unbekannter Teil 5 mit 0x05, Länge: 6 Bytes, Offset: 134
            time_offset,       # Zeitoffset, Länge: 2 Bytes (short), Offset: 140
        )
        # Sounding_Data hinzufügen
        block += sounding_data
        return block  

    def _encode_sounding_data(self, sounding_data):
        """Kodiert die Sounding-Daten als binären Block."""
        sounding_data = sounding_data.strip('[]').strip()
        # Konvertieren der Sounding-Daten von CSV-String in Bytes
        sounding_values = [int(x) for x in sounding_data.split(';')]
        return struct.pack(f'<{len(sounding_values)}B', *sounding_values)

# Beispielaufruf
input_file = r'C:\Users\ssteinhauser\Masterthesis\LowFake\Decoder\sl2ToCsvOutput.csv'
output_file = r'C:\Users\ssteinhauser\Masterthesis\LowFake\Encoder\csvTosl2.sl2'
encoder = SL2Encoder(input_file, output_file)
encoder.load_csv()
encoder.encode()
