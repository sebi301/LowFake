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
        total_rows = 0
        invalid_rows = 0
        with open(self.csv_filepath, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                total_rows += 1
                if self._is_valid_row(row):
                    self.records.append(row)
                else:
                    invalid_rows += 1    
        # Debugging: Anzahl der geladenen Zeilen anzeigen
        print(f"Anzahl der vorhandenen Datensätze: {total_rows}")
        print (f"Anzahl der ungültigen Datensätze: {invalid_rows}")
        print(f"Anzahl der der validen Datensätze: {len(self.records)}")
        
                    

    def _is_valid_row(self, row):
        """Prüft, ob eine Zeile valide ist."""
        # Prüfen, ob die Spalten `sounding_*` oder `0.010407008` existieren und mindestens eine davon gefüllt ist, egal ob mit Daten oder `[]`
        sounding_columns = [key for key in row.keys() if key.startswith("sounding_") or key == "0.010407008"]

        if not sounding_columns:
            print("Ungültige Zeile (Keine Sounding-Daten erkannt):", row)
            return False

        # Prüfen, ob mindestens eine Sounding-Spalte gefüllt ist (nicht leer und nicht [])
        has_valid_sounding_data = any(
            row[col].strip() and row[col].strip() != "[]" for col in sounding_columns
        )

        if not has_valid_sounding_data:
            print("Ungültige Zeile (Alle Sounding-Spalten sind leer oder []):", row)
            return False

        try:
            water_depth = float(row.get('water_depth', '0'))
            if water_depth > 500 or water_depth <= 0.3:
                print("Ungültige Zeile (Water Depth außerhalb gültiger Werte):", row)
                return False

            speed_gps = float(row.get('speed_gps', '0'))
            if speed_gps > 100 or speed_gps < 0:
                print("Ungültige Zeile (Speed GPS außerhalb gültiger Werte):", row)
                return False

            block_size = int(row.get('block_size', '0'))
            if block_size > 2064:
                print("Ungültige Zeile (Block Size zu groß):", row)
                return False

        except ValueError as e:
            print(f"ValueError in Zeile {row}: {e}")
            return False
        return True
    
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
        sounding_columns = [key for key in record.keys() if key.startswith("sounding_") or key == "0.010407008"]
        sounding_data = []
        for col in sounding_columns:
            try:
                values = record[col].strip("[]").split(";")  # Entfernt [] und splittet Zahlen
                sounding_data.extend([int(x) for x in values if x.strip().isdigit()])  # Konvertiert zu Integern
            except ValueError:
                print(f" Fehler beim Parsen von Sounding-Daten in Spalte {col}, Zeile {index}: {record[col]}")
                return None  # Falls Fehler, erstelle keinen Block

        # Prüfen, ob Sounding-Daten existieren
        if not sounding_data:
            print(f" Warnung: Sounding-Daten leer für Zeile {index}")
            return None  # Kein Block wird erstellt
        
        # Beispielwerte, angepasst basierend auf den Dokumentationen
        frame_offset = int(record['frame_offset'])
        prim_last_channel_frame_offset = int(record['prim_last_channel_frame_offset'])
        sec_last_channel_frame_offset = int(record['sec_last_channel_frame_offset'])
        downscan_last_channel_frame_offset = int(record['downscan_last_channel_frame_offset'])
        side_left_last_channel_frame_offset = int(record['side_left_last_channel_frame_offset'])
        side_right_last_channel_frame_offset = int(record['side_right_last_channel_frame_offset'])
        composite_last_channel_frame_offset = int(record['composite_last_channel_frame_offset'])
        block_size = int(record['block_size'])
        last_block_size = int(record['last_block_size'])
        channel = int(record['channel'])
        packet_size = int(record['packet_size'])
        frame_index = int(record['frame_index'])
        upper_limit = float(record['upper_limit'])
        lower_limit = float(record['lower_limit'])
        unknownPart2 = bytes([0x00, 0x0C, 0x04, 0x13, 0x10])#bytes([0x02]*5) [12, 04, 19, 01]
        frequency = int(float(record['frequency']))
        unknownPart3 = bytes([0x00, 0x29, 0x00, 0x01, 0x00, 0x01]) #bytes([0x00]*6) [00 41, 00 01, 00 01]
        time1 = int(record['time1'])
        water_depth = float(record['water_depth'])
        keel_depth = float(record['keel_depth'])
        unknownPart4 = bytes([0x04, 0x04, 0x13, 0x05, 0x01, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x01, 0x01, 0x01, 0x01]) #bytes([0x04]*28) #0x1305 geteilt in 0x13 und 0x05, war vielleicht falsch, weil hex statt dezimal
        speed_gps = float(record['speed_gps'])
        temperature = float(record['temperature'])
        latitude = float(record['latitude'])
        longitude = float(record['longitude'])
        speed_water = float(record['speed_water'])
        course_over_ground = float(record['course_over_ground'])
        altitude = float(record['altitude'])
        heading = float(record['heading'])
        flags = int(record['flags'])
        unknowPart5 = bytes([0x00, 0x01, 0x01, 0x0D, 0x0A, 0x01]) #bytes([0x05]*6)
        time_offset = int(float(record['time_offset']))
        encoded_sounding_data = struct.pack(f'<{len(sounding_data)}B', *sounding_data)

        # Konvertierungen der csv-Daten in die korrekten Datentypen und Werte
        # TODO: Von Lat, Lng in Spherical Mercator Projection umrechnen
        # TODO: Von Geschwindigkeit [km/h] in Knoten umrechnen
        # TODO: Von Meter in Fuß umrechnen

        
        print(f"block_size={block_size}, last_block_size={last_block_size}, channel={channel}, "
        f"packet_size={packet_size}, frame_index={frame_index}, upper_limit={upper_limit}, "
        f"lower_limit={lower_limit}, frequency={frequency}, time1={time1}, water_depth={water_depth}, "
        f"speed_gps={speed_gps}, temprature={temperature}, latitude={latitude}, longitude={longitude}, "
        f"speed_water={speed_water}, course_over_ground={course_over_ground}, altitude={altitude}, "
        f"heading={heading}, flags={flags}, time_offset={time_offset}")
        
        # Blockstruktur erstellen
        block = struct.pack(
            '<iiiiiiihhhhiff5sb6siff28sffffffffh6si',
            frame_offset,      # Frameoffset, Länge: 4 Bytes (int), Offset: 0
            prim_last_channel_frame_offset,  # Primärer letzter Kanal Frameoffset, Länge: 4 Bytes (int), Offset: 4
            sec_last_channel_frame_offset,   # Sekundärer letzter Kanal Frameoffset, Länge: 4 Bytes (int), Offset: 8
            downscan_last_channel_frame_offset,  # Downscan letzter Kanal Frameoffset, Länge: 4 Bytes (int), Offset: 12
            side_left_last_channel_frame_offset,  # Side Left letzter Kanal Frameoffset, Länge: 4 Bytes (int), Offset: 16
            side_right_last_channel_frame_offset, # Side Right letzter Kanal Frameoffset, Länge: 4 Bytes (int), Offset: 20
            composite_last_channel_frame_offset,  # Composite letzter Kanal Frameoffset, Länge: 4 Bytes (int), Offset: 24
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
            keel_depth,        # Kiel-Tiefe, Länge: 4 Bytes (float), Offset: 68
            unknownPart4,      # Unbekannter Teil 4 mit 0x04, Länge: 28 Bytes, Offset: 68
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
            time_offset,       # Zeitoffset, Länge: 4 Bytes (int), Offset: 140 
        ) + encoded_sounding_data
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
