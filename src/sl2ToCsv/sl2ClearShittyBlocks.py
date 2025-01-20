import struct


# Warning! Script is not working properly. It is only writing the header to the new file.

class SL2Filter:
    def __init__(self, input_filepath, output_filepath):
        self.input_filepath = input_filepath
        self.output_filepath = output_filepath
        self.block_size = 2064  # Defined block size for SL2

    def _is_valid_block(self, block):
        """Validates a block based on predefined conditions."""
        try:
            block_size = struct.unpack('<H', block[28:30])[0]
            last_block_size = struct.unpack('<H', block[30:32])[0]
            channel = struct.unpack('<H', block[32:34])[0]
            packet_size = struct.unpack('<H', block[34:36])[0]
            upper_limit = struct.unpack('<f', block[40:44])[0]
            lower_limit = struct.unpack('<f', block[44:48])[0]
            frequency = block[50]
            water_depth = struct.unpack('<f', block[64:68])[0]
            speed_gps = struct.unpack('<f', block[100:104])[0]

            # Validation conditions
            if block_size > 2064 or block_size <= 0:
                return False
            if last_block_size > 2064:
                return False
            if packet_size > 2064 or packet_size <= 0:
                return False
            if water_depth <= 0.3 or water_depth > 500:  # Depth must be realistic
                return False
            if speed_gps < 0 or speed_gps > 100:  # Speed must be in a reasonable range
                return False
        except struct.error:
            return False  # If unpacking fails, block is invalid

        return True

    def filter_sl2(self):
        """Reads an SL2 file, filters invalid blocks, and writes valid blocks to a new file."""
        with open(self.input_filepath, 'rb') as infile, open(self.output_filepath, 'wb') as outfile:
            header = infile.read(10)  # SL2 files have a 10-byte header
            outfile.write(header)

            while True:
                block = infile.read(self.block_size)
                if len(block) < self.block_size:
                    break  # End of file

                if self._is_valid_block(block):
                    outfile.write(block)

if __name__ == "__main__":
    input_file = r"C:\Users\ssteinhauser\Masterthesis\LowFake\Temp\Chart 03_08_2005 [0].sl2"
    output_file = r"C:\Users\ssteinhauser\Masterthesis\LowFake\Temp\FilteredChart.sl2"

    filterer = SL2Filter(input_file, output_file)
    filterer.filter_sl2()
    print(f"Filtered SL2 file saved as: {output_file}")
