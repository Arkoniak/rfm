import tarfile
import argparse
import logging
import os
import math
import csv

class ProcessTar:
    def __init__(self, input_file, output_dir, header):
        self.input_file = input_file
        self.output_dir = output_dir
        self.header = header

        # aggregation tables
        self.user_id_agg = {}
        self.category_agg = {}
        self.geo_agg = {}
        self.geo_category_agg = {}

    def read_and_agg(self):
        with tarfile.open(self.input_file, 'r|*') as tar:
            for tarinfo in tar:
                processed_size = 0
                current_percentage = 0
                data = tar.extractfile(tarinfo)
                for line in data:
                    processed_size += len(line)
                    percentage = math.floor((processed_size*100/tarinfo.size)/5)*5
                    if percentage != current_percentage:
                        current_percentage = percentage
                        logging.info("Processed: {}%".format(current_percentage))
                    self.process_line(line)
                data.close()

    def save(self):
        outputs = [
            ['user_id_agg.tsv', self.user_id_agg, ['user_id', 'clicks_total', 'price_total', 'last_ts', 'last_geo']],
            ['category_agg.tsv', self.category_agg, ['category', 'clicks_total', 'price_total']],
            ['geo_agg.tsv', self.geo_agg, ['geo', 'clicks_total', 'price_total']],
            ['geo_category_agg.tsv', self.geo_category_agg, ['geo', 'category', 'clicks_total', 'price_total']]
        ]

        for output in outputs:
            filename = os.path.join(self.output_dir, output[0])
            self.save_file(filename, output[1], output[2])

    def save_file(self, filename, dict_agg, headline):
        with open(filename, 'w') as f:
            out_writer = csv.writer(f, delimiter='\t')
            try:
                if self.header == 'Y':
                    out_writer.writerow(headline)
                out_writer.writerows(dict_agg.values())
            except csv.Error as e:
                logging.error('file {}, line {}: {}'.format(filename, out_writer.line_num, e))

    def process_line(self, line):
        try:
            fields = line.split('\t')
            if fields[2] == 'click':
                log_ts = fields[0]
                user_id = fields[1]
                geo_id = fields[3]
                category = fields[4]
                price = int(fields[5])

                # user_id aggregaton
                user_id_info = self.user_id_agg.get(user_id, [user_id, 0, 0, None, None])
                user_id_info[1] += 1
                user_id_info[2] += price
                if log_ts > user_id_info[3]:
                    user_id_info[3] = log_ts
                    user_id_info[4] = geo_id
                self.user_id_agg[user_id] = user_id_info

                # category aggregation
                info = self.category_agg.get(category, [category, 0, 0])
                info[1] += 1
                info[2] += price
                self.category_agg[category] = info

                # geo aggregation
                info = self.geo_agg.get(geo_id, [geo_id, 0, 0])
                info[1] += 1
                info[2] += price
                self.geo_agg[geo_id] = info

                # geo category aggregation
                info = self.geo_category_agg.get((geo_id, category), [geo_id, category, 0, 0])
                info[2] += 1
                info[3] += price
                self.geo_category_agg[(geo_id, category)] = info

        except IndexError as e:
            logging.error("Wrong data in logs, line contains wrong number of fields: {}".format(line))

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--input-file', help='File to process. Should be tar.gz with single file inside', dest='input_file', required=True)
    parser.add_argument('-o', '--output-dir', help='Dir for output files. Current directory by default', dest='output_dir', default='.')
    parser.add_argument('--header', help='Add header row to output tsv files. No header by default', choices=['Y', 'N'], default='N', dest='header')
    parser.add_argument('--verbosity', help='Show logging info. Increased number means increased verbosity. Default is no info', choices=[1, 2], default=1, dest='verbosity', type=int)

    args = parser.parse_args()
    input_file = args.input_file
    output_dir = args.output_dir
    header = args.header
    verbosity = args.verbosity
    if verbosity == 2:
        logging.basicConfig(level = logging.INFO, format='%(asctime)s\t%(levelname)s:\t%(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    else:
        logging.basicConfig(level = logging.ERROR, format='%(asctime)s\t%(levelname)s:\t%(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    tar_processor = ProcessTar(input_file, output_dir, header)
    tar_processor.read_and_agg()
    tar_processor.save()

if __name__ == '__main__':
    main()
