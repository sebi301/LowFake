import struct
import csv
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DECODER_DIR = BASE_DIR / "decoderDocs"
ENCODER_DIR = BASE_DIR / "encoderDocs"

input_file = DECODER_DIR / "sl2ToCsvOutput_Chart_03082005.csv"
output_file = ENCODER_DIR / "csvTosl2_Chart_03082005.sl2"

POLAR_EARTH_RADIUS = 6356752.3142  # Radius der Erde für die Mercator-Umrechnung

def detect_csv_format(csv_filepath):
    """Erkennt, ob die CSV-Datei im Deeper- oder Lowrance-Format ist."""
    with open(csv_filepath, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)

        if "UnixTimestamp" in header and "Depth" in header and "Spd_kmh" in header:
            return "deeper"
        elif "time1" in header and "water_depth" in header and "latitude" in header:
            return "lowrance"
        else:
            return "unknown"

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
    
class LowranceCSVConverter:
    """Lädt eine Lowrance-CSV-Datei und gibt die Daten unverändert weiter."""

    def __init__(self, csv_filepath):
        self.csv_filepath = csv_filepath
        self.records = []

    def load_csv(self):
        """Lädt die Lowrance-CSV-Datei."""
        with open(self.csv_filepath, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if SL2Encoder._is_valid_row(row, format_type="lowrance"):
                    row["sounding_data"] = SL2Encoder._encode_sounding_data(row)
                    self.records.append(row)
        print(f"Anzahl der Lowrance-Datensätze geladen: {len(self.records)}")

class DeeperCSVConverter:
    """Konvertiert eine Deeper-CSV-Datei in ein SL2-kompatibles Datenformat."""

    def __init__(self, csv_filepath):
        self.csv_filepath = csv_filepath
        self.records = []

    def load_csv(self):
        """Lädt die Deeper-CSV-Datei und konvertiert die Daten für SL2."""
        with open(self.csv_filepath, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                converted_row = self._encode_deeper_row(row)
                if converted_row:
                    self.records.append(converted_row)
        print(f"Anzahl der Deeper-Datensätze geladen: {len(self.records)}")

    def _encode_deeper_row(self, row):
        """Konvertiert eine einzelne Zeile aus der Deeper-CSV-Datei in SL2-kompatible Daten."""

        # Ersten Zeitstempel speichern, falls noch nicht gesetzt
        current_timestamp = int(row.get('UnixTimestamp', 0))
        if self.start_time is None:
                self.start_time = current_timestamp  # Ersten Zeitstempel merken
            
            # Berechnung von time1 als relative Zeit seit Messbeginn
        try:
            index = len(self.records)
            frame_offset = index * 144  # Fortlaufender Frame-Offset
            last_channel_frame_offset = (index - 1) * 144 if index > 0 else 0
            prim_last_channel_frame_offset = frame_offset
            sec_last_channel_frame_offset = 0
            downscan_last_channel_frame_offset = 0
            side_left_last_channel_frame_offset = 0
            side_right_last_channel_frame_offset = 0
            composite_last_channel_frame_offset = 0
            block_size = 2064
            last_block_size = block_size
            channel = 0
            packet_size = 1920  # Sounding-Daten Länge anpassen
            frame_index = index
            upper_limit = 10.0  # Dummy-Wert
            lower_limit = float(row.get('Depth', 0.0)+5) * 3.28084  # Meter zu Fuß
            frequency = 200  # Standard für Deeper
            time1 = current_timestamp
            water_depth = float(row.get('Depth', 0.0))
            keel_depth = 0.0
            speed_gps = float(row.get('Spd_kmh', 0.0)) / 1.852  # km/h in Knoten
            temperature = float(row.get('Temp', 0.0))
            latitude = int(float(row.get('Latitude', 0.0)) * 1e7)  # Mercator-Skalierung
            longitude = int(float(row.get('Longitude', 0.0)) * 1e7)
            course_over_ground = 0.0
            altitude = 0.0
            heading = 0.0
            flags = 0
            time_offset = current_timestamp - self.start_time

            # Extrahiere Sounding-Daten aus allen Spalten, die mit "0." beginnen
            sounding_columns = [col for col in row.keys() if col.replace(".", "").isdigit()]
            sounding_data = []
            for col in sounding_columns:
                values = row[col].strip("[]").split(",")
                sounding_data.extend([int(float(x)) for x in values if x.strip()])

            if not sounding_data:
                print(f"Keine gültigen Sounding-Daten in Zeile {frame_index}")
                return None

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
                "last_channel_frame_offset": last_channel_frame_offset,
                "channel": channel,
                "packet_size": packet_size,
                "frame_index": frame_index,
                "upper_limit": upper_limit,
                "lower_limit": lower_limit,
                "frequency": frequency,
                "time1": time1,
                "water_depth": water_depth,
                "keel_depth": keel_depth,
                "speed_gps": speed_gps,
                "temperature": temperature,
                "latitude": latitude,
                "longitude": longitude,
                "course_over_ground": course_over_ground,
                "altitude": altitude,
                "heading": heading,
                "flags": flags,
                "time_offset": time_offset,
                "sounding_data": sounding_data
            }
        except Exception as e:
            print(f"Fehler bei der Konvertierung einer Deeper-Zeile: {e}")
            return None

class SL2Encoder:
    """Erstellt eine SL2-Datei aus vorbereiteten SL2-Daten."""

    def __init__(self, records, sl2_filepath):
        self.records = records
        self.sl2_filepath = sl2_filepath

    def encode(self):
        """Kodiert die Daten und schreibt sie in eine SL2-Datei."""
        with open(self.sl2_filepath, 'wb') as f:
            f.write(struct.pack('<IIH', 2, 1970, 8))
            for i, record in enumerate(self.records):
                block = self._create_block(record, i)
                if block:
                    f.write(block)

        print(f"SL2-Datei erfolgreich gespeichert: {self.sl2_filepath}")

    def _create_block(self, record, index):
        """Erstellt einen einzelnen SL2-Block basierend auf einem Datensatz."""
        block = struct.pack(
            '<iiiiiiihhhhiff5sb6siff28sffffffffh6si',
            record["frame_offset"],
            record["block_size"],
            record["last_channel_frame_offset"],
            record["channel"],
            record["packet_size"],
            record["frame_index"],
            record["upper_limit"],
            record["lower_limit"],
            record["speed_gps"],
            record["latitude"],
            record["longitude"],
            record["course_over_ground"],
            record["altitude"],
            record["heading"],
            record["flags"],
            record["time_offset"],
            record["time1"]
        ) + record["sounding_data"]  

        return block

    @staticmethod
    def _encode_sounding_data(row):
        """Kodiert die Sounding-Daten als binären Block."""
        sounding_columns = [col for col in row.keys() if col.replace(".", "").isdigit()]
        sounding_data = []
        for col in sounding_columns:
            values = row[col].strip("[]").split(",")
            sounding_data.extend([int(float(x)) for x in values if x.strip()])
        return struct.pack(f'<{len(sounding_data)}B', *sounding_data)

    @staticmethod
    def _is_valid_row(row, format_type):
        """Überprüft, ob eine Zeile gültig ist."""
        if format_type == "deeper":
            return '0.010407008' in row or any(col.startswith("sounding_") for col in row)
        elif format_type == "lowrance":
            return "sounding_data" in row and row["sounding_data"]
        return False


# Beispielaufruf
csv_format = detect_csv_format(input_file)
converter = DeeperCSVConverter(input_file) if csv_format == "deeper" else LowranceCSVConverter(input_file) if csv_format == "lowrance" else None

if converter:
    converter.load_csv()
    output_file = os.path.splitext(input_file)[0] + ".sl2"
    sl2_encoder = SL2Encoder(converter.records, output_file)
    sl2_encoder.encode()
else:
    print("Ungültiges CSV-Format, bitte Deeper- oder Lowrance-CSV-Datei verwenden.")