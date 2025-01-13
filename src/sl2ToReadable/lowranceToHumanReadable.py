import struct
from bitarray import bitarray
import csv

class SL2Decoder:
    def __init__(self, filepath, verbose=True):
        self.filepath = filepath
        self.verbose = verbose
        self.records = []

    def decode(self):
        with open(self.filepath, 'rb') as f:
            data = f.read()

        pos = 0
        header = data[pos:pos + 10]
        pos += 9 #Hier sind die Quellen sich nicht einig. Manche schalgen Pos 8 oder 10 vor.

        # Decode the header
        file_format = struct.unpack('<H', header[0:2])[0]

        if file_format not in range(1, 4):
            raise ValueError("Invalid 'format' in header; Likely not an SLG/SL2/SL3 file")

        version = struct.unpack('<H', header[2:4])[0]
        block_size = struct.unpack('<H', header[4:6])[0]

        if block_size not in [1970, 3200]:
            raise ValueError("Block size is not 'downscan' or 'sidescan'; Likely not an SLG/SL2/SL3 file")

        if self.verbose:
            print(f"Format: {['slg', 'sl2', 'sl3'][file_format - 1]}")
            print(f"Block size: {'downscan' if block_size == 1970 else 'sidescan'}")

        # Read the records
        while pos < len(data):
            if self.verbose and (len(self.records) % 100 == 0):
                print('.', end='', flush=True)

            try:
                # Decode record fields
                block_size = struct.unpack('<H', data[pos + 28:pos + 30])[0]
                prev_block_size = struct.unpack('<H', data[pos + 30:pos + 32])[0]
                packet_size = struct.unpack('<H', data[pos + 34:pos + 36])[0]
                frame_index = struct.unpack('<I', data[pos + 36:pos + 40])[0]

                sounding_data = self._extract_sounding_data(data[pos + 145:pos + 145 + block_size])

                record = {
                    'channel': self._decode_channel(struct.unpack('<H', data[pos + 32:pos + 34])[0]),
                    'upper_limit': struct.unpack('<f', data[pos + 40:pos + 44])[0],
                    'lower_limit': struct.unpack('<f', data[pos + 44:pos + 48])[0],
                    #'frequency': self._decode_frequency(data[pos + 50]),
                    'water_depth': struct.unpack('<f', data[pos + 64:pos + 68])[0],
                    #'keel_depth': struct.unpack('<f', data[pos + 68:pos + 72])[0],
                    'speed_gps': struct.unpack('<f', data[pos + 100:pos + 104])[0],
                    #'temperature': struct.unpack('<f', data[pos + 104:pos + 108])[0],
                    'lng_enc': struct.unpack('<i', data[pos + 108:pos + 112])[0],
                    'lat_enc': struct.unpack('<i', data[pos + 112:pos + 116])[0],
                    'speed_water': struct.unpack('<f', data[pos + 116:pos + 120])[0],
                    #'track': struct.unpack('<f', data[pos + 120:pos + 124])[0],
                    #'altitude': struct.unpack('<f', data[pos + 124:pos + 128])[0],
                    #'heading': struct.unpack('<f', data[pos + 128:pos + 132])[0],
                    'time_offset': struct.unpack('<i', data[pos + 140:pos + 144])[0],
                    #'flags': self._decode_flags(data[pos + 132:pos + 134]),
                    'sounding_data': sounding_data
                }

                self.records.append(record)
                pos += packet_size + 144

            except (struct.error, IndexError) as e:
                print(e)
                break

        if self.verbose:
            print("\nDecoding completed.")

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



    
    '''def save_to_csv(self, output_path):
        with open(output_path, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.records[0].keys())
            writer.writeheader()
            writer.writerows(self.records)'''                

# Example usage
# decoder = SL2Decoder('path_to_file.sl2')
# decoder.decode()
# decoder.save_to_csv('output.csv')
# from https://wiki.openstreetmap.org/wiki/SL2 and https://gitlab.com/hrbrmstr/arabia

decoder = SL2Decoder(r"C:\Users\ssteinhauser\Masterthesis\LowFake\Temp\Chart 03_08_2005 [0].sl2")
decoder.decode()
decoder.save_to_csv(r'c:\Users\ssteinhauser\Masterthesis\LowFake\Tempsl2ToCsvOutput.csv')