import struct
import os

class SL2Decoder:
    def __init__(self, filepath):
        self.filepath = filepath

    def decode(self):
        with open(self.filepath, 'rb') as f:
            self._validate_file(f)
            self._decode_headers(f)
            self._decode_sounding_data(f)

    def _validate_file(self, file):
        '''file.seek(0)
        magic = file.read(4)
        if magic != b'SL2\x00':
            raise ValueError("Invalid SL2 file format")'''
        file.seek(0)
        print(file.read(64))  # Lese die ersten 64 Bytes zur Analyse
        file.seek(0)
        magic = file.read(4)
        print(f"Magic header: {magic}")
        if magic != b'SL2\x00':
            raise ValueError("Invalid SL2 file format")

    def _decode_headers(self, file):
        file.seek(0)
        self.headers = []

        while True:
            try:
                block = file.read(48)  # Updated header size based on additional fields
                if not block:
                    break

                # Unpacking additional fields
                (timestamp, depth, temperature, signal_strength,
                 record_type, data_length, latitude, longitude) = struct.unpack('<IfffHHdd', block[:32])

                self.headers.append({
                    'timestamp': timestamp,
                    'depth': depth,
                    'temperature': temperature,
                    'signal_strength': signal_strength,
                    'record_type': record_type,
                    'data_length': data_length,
                    'latitude': latitude,
                    'longitude': longitude
                })
            except struct.error:
                break

    def _decode_sounding_data(self, file):
        self.sounding_data = []
        while True:
            try:
                block = file.read(512)  # Example block size for sounding data
                if not block:
                    break

                sounding_block = struct.unpack('<' + 'H' * (len(block) // 2), block)
                self.sounding_data.append(sounding_block)
            except struct.error:
                break

    def export_to_csv(self, output_path):
        import csv
        with open(output_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['timestamp', 'depth', 'temperature', 'signal_strength', 'record_type', 'data_length', 'latitude', 'longitude'])
            for header in self.headers:
                writer.writerow([
                    header['timestamp'],
                    header['depth'],
                    header['temperature'],
                    header['signal_strength'],
                    header['record_type'],
                    header['data_length'],
                    header['latitude'],
                    header['longitude']
                ])

# Example usage:
# decoder = SL2Decoder('path_to_sl2_file.sl2')
# decoder.decode()
# decoder.export_to_csv('output.csv')

decoder = SL2Decoder(r'C:\Users\ssteinhauser\Masterthesis\Rohdaten\Lowrance_Hook\Chart 03_08_2005 [0].sl2')
decoder.decode()
decoder.export_to_csv(r'C:\Users\ssteinhauser\Masterthesis\LowFake"\output.csv')
