#!/usr/bin/python
import argparse
import csv
import os
import json
import requests
import xml.etree.ElementTree

ENV_ENDPOINTS = {
  'local': 'minted.local',
  'test': 'minted-api2.test.mntd.net'
}
ENV_FOREMAN_ENDPOINT = {
  'local': '10.33.33.102',
  'test': 'di-jobs-test.mntd.net'
}

class Parser(object):
  def __init__(self, file):
    self.file = file

  def yield_params(self):
    raise NotImplementedError

class XMLParser(Parser):
  def yield_params(self):
    e = xml.etree.ElementTree.parse(self.file).getroot()
    for row in e.findall('row'):
      fields = row.findall('field')
      yield fields[0].text, fields[1].text

class CSVParser(Parser):
  def yield_params(self):
    with open(self.file, 'rb') as csvfile:
      reader = csv.DictReader(csvfile, delimiter=',')
      for row in reader:
        yield row['id'], row['parameters']

def get_parser(input_file):
  name, ext = os.path.splitext(input_file)
  if ext == '.csv':
    return CSVParser(input_file)
  elif ext == '.xml':
    return XMLParser(input_file)

# data = {'kind': 'di_media_creator', 'arguments': {'endpoint': 'minted.local', 'job_id': 1575L, 'parameters': '{"media_job_type": 1, "orientation": "S", "die_cut_id": None, "event_id": 7572, "scene7_url": "http://api-s7.mintedcdn.net/is/agm/Minted/MIN-644-DCM_A_FRT?setElement.text1={<content>Let%E2%80%99s%2520Party!</content>}&setElement.text2={<content>COOKOUT%2520AND%2520POOL%2520PARTY</content>}&setElement.text3={<content>SATURDAY%2520MAY%25202%2520%E2%80%A2%25201PM%2520UNTIL%2520DUSK</content>}&setElement.text4={<content>THE%2520TEDFORDS%2520%E2%80%A2%2520472%2520ROBIN%2520COURT%2520%E2%80%A2%2520MEDFORD</content>}&wid=600&imageRes=150&qlt=90,1&fmt=png-alpha&", "create_invitation_media": false, "die_cut_name": None, "background_image_url": "http://mintedcdn.s3.amazonaws.com/files/mintedProductsImages/di/backgrounds/media/32630c68b4f673cf1e0a6c2a6dc8956af335d20c/2015_02_09_difiesta-038-21.jpg?Signature=dwH7bDWOE404O95gXBIhkajQlKo%3D&Expires=1461349300&AWSAccessKeyId=AKIAJCKWDVE4Z3CCRBPA&versionId=Lk1OhlSX553grRiiLDBN7f84Gm4Kmh7T", "fxg_filename": "MIN-644-DCM_A_FRT", "saved_design_template_id": 62635340, "desktop_frame_width": 800, "is_generate": true}'}}
# data = {"media_job_type": 1, "orientation": "P", "die_cut_id": None, "event_id": 5430, "scene7_url": "http://api-s7.mintedcdn.net/is/agm/Minted/MIN-648-DCT_A_FRT?setElement.text1={<content>YOU%2520ARE%2520INVITED%2520TO%2520A</content>}&setElement.text2={<content>COCKTAIL%2520PARTY</content>}&setElement.text3={<content>SATURDAY</content>}&setElement.text4={<content>JUNE%252020TH</content>}&setElement.text5={<content>The%2520%C3%AF%25202014</content>}&setElement.text6={<content>224%2520WISTERIA%2520DRIVE</content>}&setElement.text7={<content>NEW%2520ORLEANS%2C%2520LA%252070124</content>}&setElement.text8={<content>HOSTED%2520BY%2520OLIVER%2520%252B%2520ESTELLE%2520HARRISON</content>}&wid=600&imageRes=150&qlt=90,1&fmt=png-alpha&", "create_invitation_media": False, "die_cut_name": None, "background_image_url": "http://mintedcdn.s3.amazonaws.com/files/mintedProductsImages/di/backgrounds/media/3ef8a3e66d70aadb19b516ac585528a8093b54ad/Nov-Minted-426.jpg?Signature=NIBnlIJpX4QqRAu2WQj7Olss%2FRs%3D&Expires=1461352027&AWSAccessKeyId=AKIAJCKWDVE4Z3CCRBPA&versionId=TvpFCRjGGam_v4k1.BGss0p1FycjrToP", "fxg_filename": "MIN-648-DCT_A_FRT", "saved_design_template_id": 67750635, "desktop_frame_width": 800, "is_generate": True}
# host = 'di-jobs-test.mntd.net'
def post(host, data):
  url = 'http://{}:39243/receive'.format(host)
  headers = {'content-type': 'application/json'}

  result = requests.post(url, data=json.dumps(data), headers=headers)
  print result


def get_post_data(job_id, parameters, env):
  assert env in ENV_ENDPOINTS
  return {'kind': 'di_media_creator', 'arguments': {'endpoint': ENV_ENDPOINTS[env], 'job_id': job_id, 'parameters': parameters}}

# csv_file = 'di_dev_di_event_email_job.csv'

# for job_id, parameters in yield_id_and_parameter(csv_file):
  # data = get_post_data(job_id, parameters, 'local')
  # post(host, data)

  # import pdb; pdb.set_trace()
  # print row.get('id')
  # print row.get('parameters')
  # (Pdb) row.findall('field')[1].get('name')

if __name__ == '__main__':
  description = """
  Replay media jobs onto the appropriate foreman
  """
  parser = argparse.ArgumentParser(description=description)
  parser.add_argument('--env', type=str, choices=['local', 'test'], required=True)
  parser.add_argument('--file', type=str, required=True)
  # host = '10.33.33.102'
  # file = 'di_dev_di_event_email_job.xml'
  # parser = get_parser(file)
  # for job_id, parameters in parser.yield_params():
    # data = get_post_data(job_id, parameters, 'local')
    # post(host, data)
