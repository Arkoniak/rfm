import tarfile
import argparse
import logging
import os
import math
import csv
import pandas as pd
import numpy as np

class ProcessTar:
    def __init__(self, input_file, output_dir, header):
        self.input_file = input_file
        self.output_dir = output_dir
        self.header = header == 'Y'

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

        self.df_user_id_agg = pd.DataFrame(self.user_id_agg.values())
        self.df_category_agg = pd.DataFrame(self.category_agg.values())
        self.df_geo_agg = pd.DataFrame(self.geo_agg.values())
        self.df_geo_category_agg = pd.DataFrame(self.geo_category_agg.values())

    def save(self):
        outputs = [
            ['user_id_agg.tsv', self.df_user_id_agg,
                ['user_id', 'clicks_total', 'price_total', 'last_ts', 'last_geo'],
                [('rfm_last_ts', 'last_ts'), ('rfm_clicks_total', 'clicks_total'), ('rfm_price_total', 'price_total')]],
            ['category_agg.tsv', self.df_category_agg,
                ['category', 'clicks_total', 'price_total'],
                [('rfm_clicks_total', 'clicks_total'), ('rfm_price_total', 'price_total')]],
            ['geo_agg.tsv', self.df_geo_agg,
                ['geo', 'clicks_total', 'price_total'],
                [('rfm_clicks_total', 'clicks_total'), ('rfm_price_total', 'price_total')]],
            ['geo_category_agg.tsv', self.df_geo_category_agg,
                ['geo', 'category', 'clicks_total', 'price_total'],
                [('rfm_clicks_total', 'clicks_total'), ('rfm_price_total', 'price_total')]]
        ]

        for output in outputs:
            filename = os.path.join(self.output_dir, output[0])
            for fields_pair in output[3]:
                df = output[1]
                new_field = fields_pair[0]
                rfm_field = fields_pair[1]
                rank_series = df[rfm_field].rank()
                max_rank = rank_series.max()
                df[new_field] = np.ceil(5*rank_series/max_rank).astype(int)
                output[2].append(new_field)

            output[1].to_csv(filename, sep='\t', header=self.header, index=False, columns=output[2])

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
                info = self.user_id_agg.get(user_id, {'user_id': user_id, 'clicks_total': 0, 'price_total': 0, 'last_ts': None, 'last_geo': None})
                info['clicks_total'] += 1
                info['price_total'] += price
                if log_ts > info['last_ts']:
                    info['last_ts'] = log_ts
                    info['last_geo'] = geo_id
                self.user_id_agg[user_id] = info

                # category aggregation
                info = self.category_agg.get(category, {'category': category, 'clicks_total': 0, 'price_total': 0})
                info['clicks_total'] += 1
                info['price_total'] += price
                self.category_agg[category] = info

                # geo aggregation
                info = self.geo_agg.get(geo_id, {'geo': geo_id, 'clicks_total': 0, 'price_total': 0})
                info['clicks_total'] += 1
                info['price_total'] += price
                self.geo_agg[geo_id] = info

                # geo category aggregation
                info = self.geo_category_agg.get((geo_id, category), {'geo': geo_id, 'category': category, 'clicks_total': 0, 'price_total': 0})
                info['clicks_total'] += 1
                info['price_total'] += price
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
