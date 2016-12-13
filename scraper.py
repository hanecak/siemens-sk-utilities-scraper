#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2016, Peter Hanecak <hanecak@opendata.sk>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# This screpers takes data from
# www.siemens.sk/sites/osvetlenie/osvetlenie_pac.php taking advantage of the
# Ajax back-end of that page which is used to provide data (in XML) for the
# map visualization of the utilities. So, we just get the data at once for
# whole slovakia (passing a proper bounding box) and then parse the XML
# result.

import datetime
import httplib, urllib
import lxml.etree
import scraperwiki
import sys

REQUEST_HOST = '192.108.125.143'
REQUEST_URI = '/geoserver/wfs'
REQUEST_HEADERS = {'Content-type': 'application/xml'}

REQUEST_BODY = """<wfs:GetFeature xmlns:wfs="http://www.opengis.net/wfs" service="WFS" version="1.1.0" xsi:schemaLocation="http://www.opengis.net/wfs http://schemas.opengis.net/wfs/1.1.0/wfs.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <wfs:Query typeName="feature:frm" srsName="EPSG:4326" xmlns:feature="http://www.slm.lighting">
    <ogc:Filter xmlns:ogc="http://www.opengis.net/ogc">
      <ogc:BBOX>
        <gml:Envelope xmlns:gml="http://www.opengis.net/gml" srsName="EPSG:4326">
          <gml:lowerCorner>%s %s</gml:lowerCorner>
          <gml:upperCorner>%s %s</gml:upperCorner>
        </gml:Envelope>
      </ogc:BBOX>
    </ogc:Filter>
  </wfs:Query>
</wfs:GetFeature>"""

# bounding box for which we're going to get the data
# ( SW lon, SW lat, NE lon, NE lat)
BBOX_TEST = ('17.0629305', '48.1211625', '17.1029305', '48.1411625')
BBOX_BA = ('16.922942', '48.041167', '17.242919', '48.221158')
BBOX_SK = ('16.77497', '47.71372', '22.62519', '49.66433')

# Morph.io schedules runs once a day.  We do not want to overload the
# Siemens server too much thus we will try to get data only once a week.
last_run = scraperwiki.sqlite.get_var('last_run')
if last_run is not None:
    tmp = datetime.datetime.strptime(last_run, "%Y-%m-%dT%H:%M:%S")
    delta = datetime.datetime.utcnow() - tmp
    if (delta.days < 7):
        print '### data is quite fresh (%s), skipping run' % last_run
        sys.exit()

# read in a data from their Ajax back-end
print '### connecting:',
conn = httplib.HTTPConnection(REQUEST_HOST)
conn.request('POST', REQUEST_URI, REQUEST_BODY % BBOX_SK, REQUEST_HEADERS)
response = conn.getresponse()
print 'done'

# iterate over response and store items
print '### parsing response:',
tree = lxml.etree.parse(response)
tree_root = tree.getroot()
item_count = 0
for feature_members in tree_root.findall('gml:featureMembers', tree_root.nsmap):
    for frame in feature_members.findall('slm:frm', tree_root.nsmap):
        frm_gml_id = frame.attrib.get('{http://www.opengis.net/gml}id')
        #print 'XXX', frm_gml_id

        fmr_slm_id = frame.find('slm:id', tree_root.nsmap).text
        fmr_slm_smid = frame.find('slm:smid', tree_root.nsmap).text
        fmr_slm_smcislo = frame.find('slm:smcislo', tree_root.nsmap).text
        fmr_slm_aktivne = frame.find('slm:aktivne', tree_root.nsmap).text

        fmr_slm_smgeopoint = frame.find('slm:smgeopoint', tree_root.nsmap)
        fmr_slm_smgeopoint_point = fmr_slm_smgeopoint.find('gml:Point', tree_root.nsmap)
        fmr_slm_smgeopoint_point_pos = fmr_slm_smgeopoint_point.find('gml:pos', tree_root.nsmap).text

        temp = fmr_slm_smgeopoint_point_pos.split(' ')
        position_lat = temp[0]
        position_lon = temp[1]

        scraperwiki.sqlite.save(
            unique_keys=['gml_id', 'slm_id', 'smcislo', 'smcislo'],
            data={
                'gml_id': frm_gml_id,
                'slm_id': fmr_slm_id,
                'smid': fmr_slm_smid,
                'smcislo': fmr_slm_smcislo,
                'aktivne': fmr_slm_aktivne,
                'position_lat': position_lat,
                'position_lon': position_lon,
                'scrap_time': datetime.datetime.utcnow().replace(microsecond=0).isoformat()
            }
        )
        item_count += 1

print 'done (%d items parsed)' % item_count

# we're done, close the connection and note run time
conn.close()
scraperwiki.sqlite.save_var('last_run', datetime.datetime.utcnow().replace(microsecond=0).isoformat())
