#!/usr/bin/python
import argparse
import csv
import os
import json
import requests
import xml.etree.ElementTree

ENV_MINTED_API_ENDPOINT = {
  'local': 'minted.local',
  'test': 'minted-api2.test.mntd.net',
  'qa': 'qa.minted.com',
  'stage': 'stage.minted.com'
}
ENV_FOREMAN_ENDPOINT = {
  'local': '10.33.33.102',
  'test': 'di-jobs-test.mntd.net',
  'qa': 'di-jobs-qa.mntd.net',
  'stage': 'di-jobs-stage.mntd.net'
}

class Parser(object):
  def __init__(self, file):
    self.file = file

  def yield_params(self):
    raise NotImplementedError

class XMLParser(Parser):
  def yield_params(self, is_plain=True):
    """
    is_plain - whether the input is in plain xml format or mysql specific format
    """
    root = xml.etree.ElementTree.parse(self.file).getroot()
    if is_plain:
      custom = root.find('custom')
      rows = custom.findall('row')
    else:
      rows = root.findall('row')
    for row in rows:
      yield self._parse_row(row, is_plain)

  def _parse_row(self, row, is_plain):
    id_value = parameters_value = None
    if is_plain:
      id_value = row.find('id').text
      parameters_value = row.find('parameters').text
    else:
      fields = row.findall('field')
      for field in fields:
        if field.attrib['name'] == 'id':
          id_value = field.text
        if field.attrib['name'] == 'parameters':
          parameters_value = field.text
    assert id_value is not None, "'id' value not found"
    assert parameters_value is not None, "'parameters' value not found"
    return id_value, parameters_value

class CSVParser(Parser):
  def yield_params(self):
    with open(self.file, 'rb') as csvfile:
      reader = csv.DictReader(csvfile, delimiter=',')
      for row in reader:
        yield row['id'], row['parameters']

def get_file_parser(input_file):
  assert os.path.exists(input_file), '{} does not exists'.format(input_file)
  name, ext = os.path.splitext(input_file)
  if ext == '.csv':
    return CSVParser(input_file)
  elif ext == '.xml':
    return XMLParser(input_file)
  else:
    raise AssertionError('unknown file format {}'.format(input_file))

def post(host, data, port=39243):
  url = 'http://{}:{}/receive'.format(host, port)
  headers = {'content-type': 'application/json'}

  return requests.post(url, data=json.dumps(data), headers=headers)
  

def get_post_data(job_id, parameters, env, job_type):
  assert env in ENV_MINTED_API_ENDPOINT
  return {'kind': job_type, 'arguments': {'endpoint': ENV_MINTED_API_ENDPOINT[env], 'job_id': job_id, 'parameters': parameters}}

if __name__ == '__main__':
  description = """
  Replay media jobs onto the appropriate foreman
  """
  parser = argparse.ArgumentParser(description=description)
  parser.add_argument('--env', type=str, choices=ENV_MINTED_API_ENDPOINT.keys(),
    required=True, help='Environment')
  parser.add_argument('--file', type=str, required=True, help='input file, either in .csv or .xml format')
  parser.add_argument('--job_type', type=str, required=True, choices=['di_media_creator', 'di_mailers'])
  args = parser.parse_args()
  file_parser = get_file_parser(args.file)
  host = ENV_FOREMAN_ENDPOINT[args.env]
  for job_id, parameters in file_parser.yield_params():
    data = get_post_data(job_id, parameters, args.env, args.job_type)
    resp = post(host, data)
    print '{} responded {} to job {}'.format(host, resp, job_id)
